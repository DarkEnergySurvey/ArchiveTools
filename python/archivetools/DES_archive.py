"""
This module contains the class information for archiving DES data into
the mass storage system.

"""
import os

import datetime
from time import strftime
import archivetools.backup_util as bu
from archivetools.DES_tarball import DES_tarball


class DES_archive(object):
    """ Class to create and archive tar ball

    """

    def __init__(self, args, util, clss, priority, verify=False):
        """ Parameters
            ----------
            args : dict
                Input options
            util : Util class instance
                Used for logging
            clss : str
                The class of data being processed
            priority : int
                The priority of the data
        """
        # Store Variables
        self.args = args
        self.priority = priority
        self.project = "DES"
        self.file_class = clss
        self.stage_dir = args['stgdir']
        self.xfer_dir = args['xferdir']
        self.archive_size = 0
        self.dir_list = []
        self.util = util
        self.archive_md5 = None
        self.tarfile = None
        self.verify = verify

        # Calculate timestamp
        self.timestamp = strftime("%Y%m%d_%H%M%S")

        self.archive_base = self.project + "_" + \
                            self.file_class + "_" + \
                            self.timestamp + ".tar"
        self.archive_name = os.path.join(self.stage_dir, self.archive_base)

        print "the archive name is " + self.archive_name
        print "the stage dir " + self.stage_dir

        self.restore()

    def change_to_staging_dir(self):
        """ Method to change to the staging directory
        """
        os.chdir(self.stage_dir)

    def make_directory_tar(self, dirname, data, path, size=0, md5sum=0):
        """ Method to generate a tarball from the given directory

            Parameters
            ----------
            dirname : str
                The name of the directory generate the tarball from

            data : dict
                Dictionary of file info

            path : str
                The full path to the directory to be processed

            Returns
            -------
            str containing the name of the tar file
        """
        mytar = DES_tarball(self.args, [dirname], data, self.util, path, size,
                            md5sum, self.file_class)
        self.dir_list.append(mytar)
        self.archive_size += mytar.tar_size
        if size == 0:
            if not self.util.ping():
                self.util.reconnect()

            self.update_db_unit(mytar, dirname)

    def make_directory_tars(self, dirs):
        """ Method to generate multiple tarballs of directories

            Parameters
            ----------
            dirs : dict
                Dictionary containing the directory and file information
        """
        for ddir in dirs:
            self.make_directory_tar("", ddir[0], "", ddir[1], ddir[2])

    def generate(self):
        """ Method to create a tarball and record its md5sum
        """
        self.util.log(bu.Util.info, "=> Generating: {0}".format(self.archive_name))
        self.change_to_staging_dir()
        data = {}
        tarfiles = []
        for idx in self.dir_list:
            data[idx.tarfile] = [None, idx.get_md5sum()]
            tarfiles.append(idx.tarfile)
        cwd = os.getcwd()
        ubertar = DES_tarball(self.args, tarfiles, data, self.util, self.stage_dir, file_class=None, verify=self.verify, tarname=self.archive_name)
        os.chdir(cwd)
        self.archive_md5 = ubertar.get_md5sum()
        for idx in self.dir_list:
            self.util.log(bu.Util.info, "===>  Removing: {0}".format(idx.tarfile))
            os.remove(idx.tarfile)
            # add up db changes
        self.util.log(bu.Util.info, "Moving %s to %s" % (os.path.join(self.archive_name), os.path.join(self.xfer_dir, self.archive_base)))
        os.rename(os.path.join(self.archive_name), os.path.join(self.xfer_dir, self.archive_base))
        os.system("chmod g+w %s" % os.path.join(self.xfer_dir, self.archive_base))
        self.util.log(bu.Util.info, "=> Tar file generated: %s" % (self.archive_name))
        if not self.util.ping():
            self.util.reconnect()

        self.update_db_tape()

    def update_db_tape(self):
        """ Method to update the DB with current status of unit and tape tars"""
        now = datetime.datetime.now()
        urows = []

        for ddir in self.dir_list:
            urows.append((ddir.tarfile, self.archive_base, ddir.tarfile, ddir.tar_size, ddir.md5sum, self.archive_base, self.file_class))
        cur = self.util.cursor()

        cur.prepare("merge into BACKUP_UNIT bu using dual on (bu.name=:1) when matched then update set tape_tar=:2,status=1 when not matched then insert (NAME,DEPRECATED,TAR_SIZE,MD5SUM,CREATED_DATE,TAPE_TAR,FILE_TYPE,STATUS) values (:3,0,:4,:5,TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),:6,:7,1)" % (now.strftime('%Y-%m-%d %H:%M:%S')))

        cur.executemany(None, urows)
        sql = "insert into BACKUP_TAPE (NAME,TAR_SIZE,CREATED_DATE,MD5SUM,RETRIES,STATUS,PATH,DEPRECATED,PRIORITY,FILE_TYPE) values ('%s',%i,TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),'%s',0,0,'%s',0,%i, '%s')" % (self.archive_base, self.archive_size, now.strftime('%Y-%m-%d %H:%M:%S'), self.archive_md5, self.xfer_dir, self.priority, self.file_class)
        print sql
        cur.execute(sql)
        cur.execute('commit')

    def update_db_unit(self, mytar, dirname):
        """ Method to update the backup_unit and backup_dir tables

            Parameters
            ----------
            mytar : DES_tarball object
            dirname : str
                The name of the directory
        """
        cur = self.util.cursor()
        cur.execute("insert into BACKUP_UNIT (NAME,DEPRECATED,TAR_SIZE,MD5SUM,CREATED_DATE,FILE_TYPE,STATUS) values ('%s',0,%i,'%s',TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),'%s',2)" % (mytar.tarfile, mytar.tar_size, mytar.md5sum, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), mytar.file_class))
        cur.execute("update BACKUP_DIR set UNIT_NAME='%s',STATUS=1 where PATH='%s'" % (mytar.tarfile, dirname))
        cur.execute('commit')

    def return_key_value(self, key):
        """ Method to get a specific internal variable

            Parameters
            ----------
            key : str
                The item to return the value of

            Returns
            -------
            varies, depends on the data type of the item
        """
        mydict = self.__dict__
        return mydict.get(key)

    def print_vars(self):
        """ Method to print the current internal variables
        """
        mydict = self.__dict__
        print "Object Variables"
        for key in iter(mydict):
            print "\t%s = %s" % (key, mydict.get(key))

    def restore(self):
        """ Method to get data of unit files which are not part of a tape tar
        """
        cur = self.util.cursor()
        cur.execute("select NAME,TAR_SIZE,MD5SUM from BACKUP_UNIT where STATUS=2 and FILE_TYPE='%s'" % (self.file_class))
        listing = cur.fetchall()
        #print listing
        self.make_directory_tars(listing)
