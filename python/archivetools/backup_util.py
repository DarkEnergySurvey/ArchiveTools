""" Utilities for backup processing
"""
import smtplib
import os
import re
import hashlib
import random
import tarfile
import shutil
import logging
from logging.handlers import TimedRotatingFileHandler
from email.mime.text import MIMEText
import matplotlib.pyplot as pyplot

import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi

CLASSES = ['finalcut', 'coadd', 'multiepoch', 'y2reproc', 'firstcut', 'supercal', 'precal', 'sne', 'prebpm', 'photoz', 'raw']
def locate(util, filename=None, reqnum=None, unitname=None, attnum=None, pfwid=None, rootpath=None, archive=None):
    """ Method to locate the unit and tape_tar files for the given inputs

        Parameters
        ----------
        util : Util instance
        filename : str
            The name of the file to locate in the backups
        reqnum : int
            The reqnum to locate in the backups (must be accompanied by unitname and attnum)
        unitname : str
            The unitname to locate in the backups (must be accompanied by reqnum and attnum)
        attnum : str
            The attnum to locate in the backups (must be accompanied by reqnum and unitname)
        pfwid : int
            The pfw_attempt_id to locate in the backups
        rootpath : str
            The path to locate in the backups
        archive : str
            The name of the archive to look in

        Returns
        -------
        Tuple containing the unit name, created date, tape tar name, created date, and
        transferred date (None, None, None, None, None) if not found
    """
    data = {'unit': None,
            'unitdate': None,
            'tape': None,
            'tapedate': None,
            'transdate': None,
            'arch_root': None,
            'path': None}
    cur = util.cursor()
    if archive:
        cur.execute("select root from ops_archive where name='%s'" % (archive))
        res = cur.fetchall()
        if res:
            data['arch_root'] = res[0][0]
    if filename:
        (fname, compression) = miscutils.parse_fullname(filename, miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION)
        if compression:
            cur.execute("select fai.path, pfw.archive_path from file_archive_info fai, desfile df, pfw_attempt pfw where df.filename='%s' and df.compression='%s' and df.pfw_attempt_id=pfw.id and fai.desfile_id=df.id" % (fname, compression))
        else:
            cur.execute("select fai.path, pfw.archive_path from file_archive_info fai, desfile df, pfw_attempt pfw where df.filename='%s' and df.compression is null and df.pfw_attempt_id=pfw.id and fai.desfile_id=df.id" % (filename))
        res = cur.fetchall()
        if not res:
            if compression:
                cur.execute("select archive_path from pfw_attempt pfw, desfile df where df.filename='%s' and df.compression='%s' and df.pfw_attempt_id=pfw.id" % (fname, compression))
            else:
                cur.execute("select archive_path from pfw_attempt pfw, desfile df where df.filename='%s' and df.pfw_attempt_id=pfw.id" % (fname))
            res = cur.fetchall()
            if not res:
                print "File %s not found in DESFILE" % (filename)
                return data
            path = res[0][0]
            print "File found in DESFILE with archive path: %s (full path to file unavailable)" % (path)
        else:
            print res
            path = res[0][1]
            print "File found with path %s and an attempt archive path of %s" % (res[0][0], path)
        data['path'] = path
    elif reqnum and unitname and attnum:
        cur.execute("select archive_path from pfw_attempt where reqnum=%s and unitname='%s' and attnum=%s" % (reqnum, unitname, attnum))
        res = cur.fetchall()
        if not res:
            print "Attempt not found in PFW_ATTEMPT"
            return data
        path = res[0][0]
        data['path'] = path
        print "Found archive path of %s for this attempt" % (path)
    elif pfwid:
        cur.execute("select archive_path from pfw_attempt where id=%s" % (pfwid))
        res = cur.fetchall()
        if not res:
            print "Attempt not found in PFW_ATTEMPT"
            return data
        path = res[0][0]
        data['path'] = path
        print "Found archive path of %s for this attempt" % (path)
    elif rootpath:
        if rootpath.startswith('/'):
            if archive:
                if not data['arch_root']:
                    print "Could not locate archive in ops_archive."
                    return data
                arch_root = res[0][0]
                path = rootpath.replace(arch_root + '/', '')
            else:
                print "Invalid path given. It must not start with a / unless an archive is also given."
                return data
        else:
            path = rootpath
        data['path'] = path
    else:
        raise ValueError("Invalid entries given")


    results = ()
    # add on an extra / at the end so the loop is easier to implement
    path += '/'
    modified = 0
    while not results:
        if path.count('/') < 1:
            print "Cannot find path in the archive, or the path is too low in the data structure."
            return data
        # remove the last directory of the path
        path = path[:path.rfind('/')]
        modified += 1
        cur.execute("select unit_name from backup_dir where path='%s'" % (path))
        results = cur.fetchall()
    if modified > 1:
        print "Found archive path of %s for given path" % (path)
    data['unit'] = results[0][0]

    if data['unit'] is None:
        print "Item %s has not been added to a backup unit yet." % (path)
        return data
    cur.execute("select CREATED_DATE,TAPE_TAR from BACKUP_UNIT where NAME='%s'" % (data['unit']))
    res = cur.fetchall()
    data['unitdate'], data['tape'] = res[0]
    if data['tape'] is None:
        return data
    cur.execute("select CREATED_DATE,TRANSFER_DATE from BACKUP_TAPE where NAME='%s'" % (data['tape']))
    data['tapedate'], data['transdate'] = cur.fetchall()[0]
    return data

def generate_md5sum(filename):
    """ Method to generate the md5sum of a file

        Parameters
        ----------
        filename : str
            The name of the file to generate the md5sum from

        Returns
        -------
        str, containing the md5sum
    """
    blksize = 2**15
    md5 = hashlib.md5()
    with open(filename, 'rb') as flh:
        for chunk in iter(lambda: flh.read(blksize), ''):
            md5.update(chunk)
    return md5.hexdigest()

def calculate_archive_size(sizestr):
    """ Method to calculate the size of an item when given the size as a string

        Parameters
        ----------
        sizestr : str
            String representation of the size (i.e. 2G)

        Returns
        -------
        int of the size in bytes
    """
    match_string = r'(\d+)(\w+)'
    regex = re.compile(match_string)
    mymatch = regex.search(sizestr)
    size = int(mymatch.group(1))
    token = mymatch.group(2)

    if token == 'b' or token == 'B':
        exponent = 0
    elif token == 'k' or token == 'K':
        exponent = 1
    elif token == 'm' or token == 'M':
        exponent = 2
    elif token == 'g' or token == 'G':
        exponent = 3
    elif token == 't' or token == 'T':
        exponent = 4
    elif token == 'p' or token == 'P':
        exponent = 5
    else:
        exponent = 0
    return size * 1024**exponent

def get_subdir(name):
    """ Method to determine the subdirectory of the file

        Parameters
        ----------
        name : str
            the name of the file

        Returns
        -------
        The subdirectory
    """
    if "RAW" in name:
        return "DTS"
    elif "DB_" in name:
        return "DB"
    return "OPS"

def srmls(server, mss_dir, subdir, fname, expected_size, util=None):
    """ Method to check if a file was successfully transferred

        Parameters
        ----------
        server : str
            The server name
        mss_dir : str
            The directory on the server
        subdir : str
            The subdirectory to use
        fname : str
            The name of the file to check
        expected_size : in
            The expected size of the file in bytes
        util : Util instance
    """
    os.system("srmls -x509_user_proxy=/home/friedel/voms.prox %s%s | grep %s > transfer.temp" % (server, os.path.join(mss_dir, subdir, fname), fname))
    tmp = open("transfer.temp", 'r')
    lines = tmp.readlines()
    if not lines:
        if util:
            util.log(Util.error, "srmls failure.")
        print "No such file on tape."
        raise SystemExit
    else:
        siz = int(lines[0].split()[0])
        print siz, expected_size, siz - expected_size
        if siz < expected_size:
            if util:
                util.log(Util.error, "Incomplete transfer")
            print "Incomplete transfer %i" % (siz)
            raise SystemExit
        return siz

def check_files(data, stagedir, archtar, util):
    """ Method to check the md5sum of the files in the tarball

        Parameters
        ----------
        data : dict
            Dictionary containing the data of the files in the tarball

        Returns
        -------
        Boolean, True if they match, False otherwise
    """
    util.log(Util.info, " ==> Checking md5sums from %s" % (archtar))
    tdir = os.sep + "tmp" + os.sep + str(random.randint(0, 100000))
    os.mkdir(tdir)
    os.chdir(tdir)
    tar = tarfile.open(os.path.join(stagedir, archtar), 'r')
    tar.extractall(tdir + os.sep)
    tar.close()
    for dirpath, _, files in os.walk(tdir):
        for fname in files:
            md5 = generate_md5sum(os.path.join(tdir, dirpath, fname))
            if md5 != data[fname][1]:
                os.chdir(stagedir)
                shutil.rmtree(tdir)
                util.log(Util.error, " ===> Bad md5sum from %s, found %s, but should be %s." % (os.path.join(tdir, dirpath, fname), md5, data[fname][1]))
                return False
    util.log(Util.info, " ===> Check complete: %s" % (archtar))
    os.chdir(stagedir)
    shutil.rmtree(tdir)
    return True


class Util(desdmdbi.DesDmDbi):
    """ Class of generic utilities for logging and db connection"""

    # logging levels
    info = 20
    warn = 30
    error = 40
    debug = 20
    cutoff = 30
    recipient = "friedel@illinois.edu"
    sender = "backups@cosmology.illinois.edu"

    def __init__(self, services, section, logfile=None, ltype=None, llevel=logging.INFO, reqfree=0, archive='desar2home'):
        """ Paramters
            ---------
            logfile : str
                The name of the log file to use
            ltype : str
                The type of log
            llevel : int
                Log at or above this logging level
            reqfree : int
                The minimum amount of free space to have on disk
        """
        if logfile is not None and ltype is not None:
            self.init_logger(logfile, ltype, llevel)
        else:
            self.logger = None
        self.services = services
        self.section = section
        desdmdbi.DesDmDbi.__init__(self, services, section)
        cur = self.cursor()
        cur.execute("select ROOT from OPS_ARCHIVE where name='%s'" % (archive))
        self.root = cur.fetchall()[0][0]

        self.reqfree = reqfree

    def ping(self):
        try:
            self.con.ping()
        except Exception:
            return False
        return True

    def reconnect(self):
        print "Reconnecting to DB."
        desdmdbi.DesDmDbi.__init__(self, self.services, self.section)

    def init_logger(self, logfile, ltype, llevel):
        """ Method to initialize the logger

            Parameters
            ----------
            logfile : str
                The name of the log file to use
            ltype : str
                The type of log
            llevel : int
                Log at or above this logging level

        """
        logging.basicConfig(level=llevel)
        self.logger = logging.getLogger(ltype)
        handler = TimedRotatingFileHandler(logfile, when="midnight")
        handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s", datefmt='%Y-%m-%d %H:%M:%S'))
        self.logger.addHandler(handler)


    def notify(self, level, msg, email=False):
        """ Method to send an email notification if there is an issue

            Parameters
            ----------
            level : int
                The level to log the message at
            msg : str
                The message to be logged
            email : bool
                If True then send an email (Default is False)
        """
        if self.logger is None:
            raise Exception("Logger not initialized, please run init_logger.")
        self.logger.log(level, msg)
        if not email and level < Util.cutoff:
            return
        email = MIMEText(msg)
        if level > Util.error:
            subj = "ERROR in backups"
        elif level > Util.warn:
            subj = "WARNING in backups"
        else:
            subj = "Message from backups"
        email['Subject'] = subj
        email['From'] = Util.sender
        email['To'] = Util.recipient
        smtp = smtplib.SMTP('localhost')
        smtp.sendmail(Util.sender, [Util.recipient], email.as_string())
        smtp.quit()

    def log(self, level, msg):
        """ Method to log a message

            Parameters
            ----------
            level : int
                The level to log the message at
            msg : str
                The message to be logged
        """
        self.notify(level, msg)

    def checkfreespace(self, dirn):
        """ Method to check the free space on disk

            Parameters
            ----------
            dirn : str
                The directory whose file system is to be checked

            Returns
            -------
            Boolean, True if there is enough free space, False otherwise
        """
        stat = os.statvfs(dirn)
        freespace = stat.f_bavail * stat.f_frsize

        if freespace < self.reqfree:
            #print freespace,options.stgdir,reqfree
            self.log(Util.error, "Not enough free space: %i need %i." % (freespace, self.reqfree))
            return False
        return True

class Plot(object):
    """ Class for making matplotlib.pyplot plots

        Parameters
        ----------
        filename : str
            The name of the output file
        title : str
            The title of the figure (default is None)
        figsize : tuple
            The size of the figure in inches (default is (6, 4))
        dpi : int
            The dots per inch of the output
    """
    number = 0
    def __init__(self, filename, title=None, figsize=(6, 4), dpi=80):
        self.filename = filename
        self.number = Plot.number
        Plot.number += 1
        self.figure = pyplot.figure(num=self.number, figsize=figsize, dpi=dpi)
        self.subplot = self.figure.add_subplot(111)
        if title:
            self.subplot.set_title(title)

    def save(self):
        """ Method to save the figure to a file
        """
        pyplot.savefig(self.filename)

    def generate(self):
        """ Placeholder, should be implemented by subclasses
        """
        print "Not implemented for this object (%s)" % (self.__class__.__name__)


class Pie(Plot):
    """ Class to generate pie charts

        Parameters
        ----------
        filename : str
            The name of the output file
        labels : list
            List of the labels for the pie sections
        sizes : list
            List of sizes of pie sections, the sum does not have to equal 1, each entry
            must have the same units
        autopct : str
            Format for the labeling of the % sizes of the sections (defualt is '%1.1f%%',
            give to only 1 decimal place)
        startangle : int
            The starting position for plotting in degrees (default is 180)
        figsize : tuple
            The size of the output figure in inches (default is (6, 4))
        dpi : int
            The dots per inch to use for the output (default is 80)
        colors : list
            The list of colors to use for the output sections (default is a wide range of colors)
    """
    def __init__(self, filename, sizes, labels, autopct='%1.1f%%', startangle=180, figsize=(6, 4), dpi=80,
                 colors=None, title=None):
        Plot.__init__(self, filename, title, figsize, dpi)
        if colors:
            self.colors = colors
        else:
            self.colors = ['grey', 'lightcoral', 'orange', 'lemonchiffon', 'lightgreen',
                           'darkcyan', 'plum', 'deeppink', 'linen', 'blanchedalmond',
                           'yellowgreen', 'lightcyan']
        if len(labels) != len(sizes):
            raise Exception("Labels is not the same length as sizes")
        self.labels = labels
        self.sizes = sizes
        self.autopct = autopct
        self.startangle = startangle

    def generate(self):
        """ Method to generate the plot
        """
        self.subplot.pie(self.sizes, labels=self.labels, autopct=self.autopct,
                         startangle=self.startangle, colors=self.colors[:len(self.labels)])
        self.subplot.axis('equal')
        self.save()


class BoxPlot(Plot):
    """ Class to generate x-y plots

        Parameters
        ----------
        filename : str
            The name of the output file
        xdata : list
            The x data points
        xlabel : str
            The label for the x axis (defualt is None)
        ylabel : str
            The label for the y axis (default is None)
        title : str
            The title to use for the plot
        figsize : tuple
            The size of the output figure in inches (default is (18, 6))
        dpi : int
            The dots per inch to use for the output (defualt is 80)
        xdate : bool
            True if the xdata are dates (default is False)
        colors : list
            The list of colors to use for the output sections (default is ['b', 'r', 'k'])
        dodots : bool
            If True the plot both lines and dots (default is False)
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, filename, xdata, xlabel=None, ylabel=None, title=None, figsize=(18, 6), dpi=80,
                 xdate=False, colors=None, dodots=False):
        Plot.__init__(self, filename, title, figsize, dpi)
        self.xdata = xdata
        self.ydata = []
        self.legend = []
        self.usedots = dodots
        self.cindex = 0
        self.xdate = xdate
        if colors:
            self.colors = colors
        else:
            self.colors = ['b', 'r', 'k']
        if xlabel:
            self.subplot.set_xlabel(xlabel)
        if ylabel:
            self.subplot.set_ylabel(ylabel)

    def add_ydata(self, ydata, legend=None):
        """ Method to add y axis data

            Parameters
            ----------
            ydata : list
                Data for the y axis
            legend : str
                Label for the legend (default is None)
        """
        self.ydata.append(ydata)
        if legend:
            self.legend.append(legend)

    def generate(self):
        """ Method to generate the plot
        """
        for i in range(len(self.ydata)):
            if self.legend:
                self.subplot.plot(self.xdata, self.ydata[i], self.colors[self.cindex] + '-', label=self.legend[i])
            else:
                self.subplot.plot(self.xdata, self.ydata[i], self.colors[self.cindex] + '-')
            if self.usedots:
                self.subplot.plot(self.xdata, self.ydata[i], self.colors[self.cindex] + 'o')
            self.cindex += 1
        if self.xdate:
            self.figure.autofmt_xdate()
        if self.legend:
            box = self.subplot.get_position()
            self.subplot.set_position([box.x0, box.y0 + box.height*0.1, box.width, box.height*0.9])
            self.subplot.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), fancybox=True,
                                shadow=True, ncol=3)
        self.save()
