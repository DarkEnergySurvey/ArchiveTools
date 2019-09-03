#!/usr/bin/env python
""" Module to create text and graphical representations of the backup tasks
"""
import time
import datetime
import subprocess as sp
import logging
import argparse
import os
import math
import pprint
import numpy as np
import shutil
import psutil

import archivetools.backup_util as bu

LOGBASE = "/local_big/backups/logs/"
MAXDT = 3600.
GREEN = '"#00FF00"'
YELLOW = '"#FFFF00"'
RED = '"#FF0000"'
WHITE = '"#FFFFFF"'
ORANGE = '"#FFA500"'

processes = [{'name': 'Setup', 'proc': "archive_setup.py", 'log': "backup_setup.log", 'maxrun': datetime.timedelta(minutes=45), 'status': None},
             {'name': 'Pack','proc': "run_backup.py", 'log': "backup.log", 'maxrun': datetime.timedelta(hours=4), 'status': None},
             {'name': 'Transfer','proc': "transfer.py", 'log': "transfer.log", 'maxrun': datetime.timedelta(hours=2), 'status': None},
             {'name': 'DB', 'proc': "run_backup_db.py", 'log': "backup_db.log", 'maxrun': datetime.timedelta(hours=16), 'status': None}]

class ProcMon(object):
    def __init__(self, status, uptime, lastrun, runcolor, status_color):
        self.status = status
        self.uptime = uptime
        self.lastrun = lastrun
        self.status_color = status_color
        self.runcolor = runcolor


def parse_options():
    """ Parse any command line options

        Returns
        -------
        Tuple containing the options and argumnts
    """
    parser = argparse.ArgumentParser(description='Monitor the backup status and software')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode')
    parser.add_argument('--des_services', action='store', help='DESDM Database Access File: %default')
    parser.add_argument('--section', action='store', help='Database to use')
    parser.add_argument('--dlen', default="14", action='store',
                        help='Length of time to produce the report for Default: %default')
    parser.add_argument('--dblen', default=6, type=int, action='store')
    return vars(parser.parse_args())

def getproc(name):
    res = []
    for proc in psutil.process_iter():
        if proc.name() == 'python':
            temp = proc.as_dict()
            if name in temp['cmdline'][1]:
                res.append(proc.as_dict())
    return res

def proc(name):
    """ Method to determine if a given task is currently running on the system

        Parameters
        ----------
        nams : str
            The name of the task to search for

        Returns
        -------
        Tuple containing a boolean (True if task is running, False otherwise) and
        the full name of the task
    """
    procs = getproc(name)
    print name,len(procs)
    if len(procs) > 1:
        raise Exception('Too many instance of %s' % name)
    elif len(procs) == 0:
        return (False, datetime.datetime.now())
    starttime = datetime.datetime.fromtimestamp(procs[0]['create_time'])
    return True,starttime


    #temp = sp.check_output(["ps", "-ef"])
    #vals = temp.split("\n")
    #for val in vals:
    #    if name in val:
    #        tmp = val.split()
    #        return True, tmp[6]
    #return False, None

def todatetime(date, time):
    """ Method to turn seperate date and time items into a datetime object

        Parameters
        ----------
        date : list
            List containing date captured from a log file
        time : list
            List containing time captured from a log file

        Returns
        -------
        datetime object
    """
    return datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), int(time[2]))

def monitorp(pname, logf, maxtime):
    """ Method to report on the current status of a task

        Paremeters
        ----------
        pname : str
            The name of the process to monitor
        logf : str
            The name of the log file for the process

        Returns
        -------
        Tuple containing the current status (str), any error messages (str),
        the last time it ran (str), the status color (str)
    """
    now = datetime.datetime.now()
    running, starttime = proc(pname)
    notime = datetime.timedelta()
    if running:
        if now - (2 * maxtime) > starttime:
            color = RED
        elif now - maxtime > starttime:
            color = YELLOW
        else:
            color = GREEN
        return ProcMon('Running', now - starttime, notime, GREEN, color)
        #return ("Running", "Uptime: %s" % (msg), None, "\"#00FF00\"")
    try:
        flh = open(LOGBASE + logf, 'r')
        rlines = flh.readlines()
        rlines.reverse()
        dellines = []
        for i in range(len(rlines)):
            last = rlines[i].split()
            try:
                start = todatetime(last[0].split("-"), last[1].split(":"))
                break
            except:
                dellines.append(i)
        rlines = [x for i, x in enumerate(rlines) if i not in dellines]
        status = True
        msg = None
        for line in rlines:
            if len(line) == 0:
                continue
            temp = line.split()
            if len(temp) == 0:
                continue
            current = todatetime(temp[0].split("-"), temp[1].split(":"))
            delta = start - current
            if abs(delta.total_seconds()) > MAXDT:
                break
            if "ERR" in temp[2].upper():
                status = False
                msg = " ".join(temp[3:])
        if status:
            if (now - start).total_seconds() < (4. * 3600.):
                color = GREEN
            else:
                color = ORANGE
            return ProcMon("IDLE", notime, start, color, WHITE)
        return ProcMon("ERROR", notime, start, RED, WHITE)
    except:
        return ProcMon("UNKNOWN", notime, notime, ORANGE, WHITE)

def get_size(path):
    """ Method to determine the size on disk of a directory

        Parameters
        ----------
        path : str
            The path to the directory

        Returns
        -------
        Int containing the szie in bytes
    """
    total_size = 0
    total_size = 0
    count = 0
    start = time.time()
    for dirpath, _, filenames in os.walk(path):
        for fname in filenames:
            count += 1
            fpath = os.path.join(dirpath, fname)
            total_size += os.path.getsize(fpath)
    print 'count  ',count,'  ',time.time()-start
    return total_size

def get_size_db(curs, pfwid):
    start = time.time()
    curs.execute('select sum(df.filesize) from desfile df, file_archive_info fai where df.pfw_attempt_id=%s and df.id=fai.desfile_id' % (pfwid))
    res = curs.fetchall()
    size = int(res[0][0])
    print size, time.time()-start
    return size

def df(path):
    """ Method to determine the size of a partition

        Parameters
        ----------
        path : str
            The path of the directory

        Returns
        -------
        Tuple containing the size of the directory in 1-K blocks (float) and
        the used space in bytes (float)
    """
    subp = sp.Popen(['df', path], stdout=sp.PIPE)
    output = subp.communicate()[0].split('\n')[1].split()
    return float(output[1]), float(output[2])

def get_tape_data(curs):
    """ Method to gather data on tar files backed up to tape

        Parameters
        ----------
        curs : cursor object

        Returns
        -------
        Tuple containing the number of transferred files, the total size of transferred files,
        number to be transferred, and the total size of the files to be transferred
    """
    curs.execute('select status, sum(tar_size), count(*) from backup_tape where status=1 or status=0 group by status')
    results = curs.fetchall()
    xfer = xfersize = nxfer = nxfersize = 0
    for res in results:
        if res[0] == 0:
            nxfer = res[2]
            nxfersize = res[1]
        else:
            xfer = res[2]
            xfersize = res[1]

    return xfer, xfersize, nxfer, nxfersize

def get_backupdir_data(cur, archive_root):
    """ Method to gather data on the directories that have been archived

        Parameters
        ----------
        cur : cursor object

        Returns
        -------
        Tuple containing number of pipeline directories processed, the number of pipeline directories
        to be processed, the number of raw directories processed, and the number of raw directories
        to be processed
    """
    print "START"
    cur.execute("select class, path from backup_dir where release_date>SYSTIMESTAMP and status=0")
    cur.execute("insert into gtt_id (select pfw_attempt_id from backup_dir where release_date>SYSTIMESTAMP and status=0")
    results = cur.fetchall()
    print "RES"
    future_raw_size = 0
    future_pipe_size = 0
    future_raw_count = 0
    future_pipe_count = 0

    future_pipe_count = len(results)
    for res in results:
        print res[1]
        if 'RAW' in res[0]:
            future_raw_size += get_size(os.path.join(archive_root, res[1]))
            future_raw_count += 1
        #else:
        #    #future_pipe_size += get_size_db(cur, res[2])
        #    future_pipe_count += 1
    cur.execute('select sum(df.filesize) from desfile df, file_archive_info fai, gtt_id gtt where df.pfw_attempt_id=gtt.id and df.id=fai.desfile_id' % (pfwid))
    future_pipe_size = int(cur.fetchall()[0][0])

    future_pipe_count -= future_raw_count
    print '2'
    cur.execute("select class, path from backup_dir where release_date<=SYSTIMESTAMP and status=0")
    results = cur.fetchall()
    print 'RES'
    raw_size = 0
    pipe_size = 0

    for res in results:
        if 'RAW' in res[0]:
            raw_size += get_size(os.path.join(archive_root, res[1]))
        else:
            pipe_size += get_size(os.path.join(archive_root, res[1]))

    cur.execute("select status, class, count(status) from backup_dir where path not like '%snmanifest%' and release_date<=SYSTIMESTAMP and status<3 group by class, status")
    results = cur.fetchall()
    print 'r3'
    pproc = 0
    ptoproc = 0
    rproc = 0
    rtoproc = 0

    for res in results:
        if 'RAW' in res[1]:
            if res[0] == 1:
                rproc = res[2]
            else:
                rtoproc = res[2]
        else:
            if res[0] == 1:
                pproc += res[2]
            else:
                ptoproc += res[2]
    raw_size /= (math.pow(1024, 3))
    future_raw_size /= (math.pow(1024, 3))
    pipe_size /= (math.pow(1024, 3))
    future_pipe_size /= (math.pow(1024, 3))

    return pproc, ptoproc, rproc, rtoproc, future_raw_size, future_pipe_size, raw_size, pipe_size, future_raw_count, future_pipe_count

def get_deprecated(cur):
    """ Method to gather data on any deprecated archived data

        Parameters
        ----------
        cur : cursor object

        Returns
        -------
        Tuple containing the number of deprecated tar files and the total size of those files
    """
    cur.execute('select count(*),sum(tar_size) from backup_unit where deprecated=1')
    results = cur.fetchall()
    (deprec, depsize) = results[0]

    return deprec, depsize

def get_database(cur):
    """ Method to gather data on backed up database files

        Parameters
        ----------
        cur : cursor object

        Returns
        -------
        Tuple containing the number of backed up db directories and their total size
    """
    cur.execute("select count(*), sum(tar_size) from backup_tape where name like '%DB_BACKUP%'")
    results = cur.fetchall()
    (dbcount, dbsize) = results[0]

    dbsize /= (math.pow(1024, 4))
    return dbcount, dbsize

def get_untransferred(cur):
    """ Method to gather data on untransferred backups

        Parameters
        ----------
        cur : cursor object

        Returns
        -------
        Tuple containing the sizes, and date information for the relevant files
    """
    cur.execute("select file_type, max(created_date), sum(tar_size) from backup_unit where status=2 group by file_type")
    results = cur.fetchall()
    untrans = {}
    for res in results:
        untrans[res[0]] = {'size': res[2],
                           'last_date': res[1]}
    return untrans

def get_total_data(cur):
    """ Method to gather data on all backup units

        Paramters
        ---------
        cur : cursor object

        Returns
        -------
        Tuple containing the total size of unit tars, total size of raw tars, total tar sizes grouped
        by file type
    """
    cur.execute("select file_type,sum(tar_size) from backup_unit where tape_tar is not null and file_type not like 'DB%' group by file_type")
    results = cur.fetchall()

    sizesbytype = {}
    totaltarsize = 0
    rawsize = 0
    for res in results:
        sizesbytype[res[0]] = res[1] / math.pow(1024., 4)
        if res[0] == 'RAW':
            rawsize = res[1]/math.pow(1024., 4)
        else:
            totaltarsize += res[1] / math.pow(1024., 4)
    cur.execute("select sum(tar_size) from backup_unit where file_type like 'DB%'")
    sizesbytype['DB'] = cur.fetchall()[0][0] / math.pow(1024., 4)
    return totaltarsize, rawsize, sizesbytype

def report_processes(html):
    """ Method to print out the currect status of all archiveing processes

        Parameters
        ----------
        html : file handle
    """
    global processes

    for proc in processes:
        proc['status'] = monitorp(proc['proc'], proc['log'], proc['maxrun'])
    #scstat = monitorp("archive_setup.py", "backup_setup.log")
    #tstat = monitorp("run_backup.py", "backup.log")
    #trstat = monitorp("transfer.py", "transfer.log")
    ##rstat = monitorp("run_backup_raw.py", "rawbackup.log")
    #dbstat = monitorp("run_backup_db.py", "backup_db.log")
    header = '<tr><td></td>'
    status = '<tr><th>Status</th>'
    uptime = '<tr><th>Uptime</th>'
    lastrun = '<tr><th>Last Run</th>'
    for proc in processes:
        header += '<th>' + proc['name'] + '</th>'
        status += '<td>%s</td>' % (proc['status'].status)
        uptime += '<td bgcolor=%s>%s</td>' % (str(proc['status'].status_color), proc['status'].uptime)
        lastrun += '<td bgcolor=%s>%s</td>' % (str(proc['status'].runcolor), proc['status'].lastrun)

    header += '</tr>\n'
    status += '</tr>\n'
    uptime += '</tr>\n'
    lastrun += '</tr>\n'
    html.write("Process Status<P>\n")
    html.write("<table border=1>\n")
    #html.write("<tr><th>Scan</th><th>Tar</th><th>Transfer</th><th>DB</th></tr>\n")
    #html.write("<tr><td bgcolor=%s title='%s'>%s</td>" % (scstat[3], scstat[1], scstat[0]))
    #html.write("<td bgcolor=%s title='%s'>%s</td>" % (tstat[3], tstat[1], tstat[0]))
    #html.write("<td bgcolor=%s title='%s'>%s</td>" % (trstat[3], trstat[1], trstat[0]))
    ##html.write("<td bgcolor=%s title='%s'>%s</td>" % (rstat[3],rstat[1],rstat[0]))
    #html.write("<td bgcolor=%s title='%s'>%s</td>" % (dbstat[3], dbstat[1], dbstat[0]))
    html.write(header)
    html.write(status)
    html.write(uptime)
    html.write(lastrun)

    #html.write("</tr>\n")
    html.write("</table>\n<P>\n")

def report_untransferred(html, untrans):
    """ Method to print out the status of untransferred files

        Parameters
        ----------
        html : file handle
        untrans : dict
            The untransferred file data
    """
    html.write('<table border=0>\n<tr><td>')
    html.write("<P>\nFiles not yet in tape tar.\n")
    html.write("<table border=1>\n")
    html.write("<tr><th>File Type</th><th>Size (G)</th><th>Last Date</th></tr>\n")
    for key, val in untrans.iteritems():
        addon = ''
        diff = (datetime.datetime.now() - val['last_date']).total_seconds()
        if diff > (24. * 14. * 3600.):
            addon = ' bgcolor=' + RED
        elif diff > (24. * 7. * 3600.):
            addon = ' bgcolor=' + YELLOW
        html.write("<tr><td>%s</td><td align='right'>%.2f</td><td%s>20%s</td></tr>\n" % (key, float(val['size'])/math.pow(1024., 3), addon, val['last_date'].strftime("%y-%m-%d")))
    html.write("</table>\n")

def report_archive_status(html, numxfer, xfersize, num_deprec, depsize, numproc, totaltarsize,
                          dbcount, dbsize, rawproc, rawsize, not_xfersize, num_not_xfer, numtoproc,
                          rawtoproc, future_raw_size, future_pipe_size, raw_size, pipe_size,
                          future_raw_count, future_pipe_count):
    """ Method to print out the archive status

        Parameters
        ----------
        html : file handle
        numxfer : int
            The number of transferred files
        xfersize : float
            The size of the transferred files in Gb
        num_deprec : int
            The number of deprecated files
        depsize : float
            The size of the deprecated files in Gb
        numproc : int
            Number of processed directories
        totaltarsize : float
            The total size of processed directories in Gb
        dbcount : int
            The number of DB directories processed
        dbsize : float
            The size of the processed Db directories
        rawproc : int
            The number of RAW directories processed
        rawsize : float
            The size of the processed RAW directories
        not_xfersize : float
            The size of files not yet transferred in Gb
        num_not_xfer : int
            The number of files not yet transferred
        numtoproc : int
            The number of directories still to be processed
        rawtoproc : int
            The number of RAW directories still to be processed
    """
    html.write("Totals only include files which have passed their release date.<P>")
    html.write("<table border=0><tr><td>")
    html.write("<table border=1>\n")
    html.write("<tr><th></th><th>Count</th><th>Size (Tb)</th></tr>\n")
    html.write("<tr><td>Tape Tars Transferred</td><td align='right'>{xf:,d}</td><td align='right'>{sz:,.3f}</td></tr>\n".format(xf=numxfer, sz=xfersize/(math.pow(1024, 4))))
    html.write("<tr><td>Units Deprecated</td><td align='right'>{dep:,d}</td><td align='right'>{sz:,.3f}</td></tr>\n".format(dep=num_deprec, sz=depsize/(math.pow(1024, 4))))
    html.write("<tr><td>Pipeline Runs Tarred</td><td align='right'>{prc:,d}</td><td align='right'>{sz:,.3f}</td></tr>\n".format(prc=numproc, sz=totaltarsize))
    html.write("<tr><td>DB Backup files</td><td align='right'>{count:,d}</td><td align='right'>{sz:,.3f}</td></tr>\n".format(count=dbcount, sz=dbsize))
    html.write("<tr><td>Nites Tarred</td><td align='right'>{prc:,d}</td><td align='right'>{sz:,.3f}</td></tr>\n".format(prc=rawproc, sz=rawsize))
    clr = GREEN
    if not_xfersize/(math.pow(1024, 4)) > 3.:
        clr = YELLOW
    if not_xfersize/(math.pow(1024, 4)) > 5:
        clr = RED
    html.write("<tr><td>Tape Tars To Transfer</td><td align='right'>{xfer:,d}</td><td bgcolor=%s align='right'>{sz:,.3f}</td></tr>\n".format(xfer=num_not_xfer, sz=not_xfersize/(math.pow(1024, 4))) % (clr))
    clr = GREEN
    if numtoproc > 100:
        clr = YELLOW
    if numtoproc > 200:
        clr = RED
    html.write("<tr><td>Pipeline Runs To Tar</td><td bgcolor=" + clr + " align='right'>" + str(numtoproc) + "</td><td align='right'>" + str(pipe_size) + "</td></tr>\n")
    clr = GREEN
    if rawtoproc > 15:
        clr = YELLOW
    if rawtoproc > 200:
        clr = RED
    html.write("<tr><td>Nites To Tar</td><td bgcolor=" + clr + " align='right'>" + str(rawtoproc) + "</td><td  align='right'>" + str(raw_size) +"</td></tr>\n")
    html.write("</table>\n<P>")
    html.write("<table border=1>\n")
    html.write("<tr><th>Future</th><th># dirs</th><th>Size (Gb)</th></tr>\n")
    html.write("<tr><td>Pipeline</td><td align='right'>" + str(future_pipe_count) + "</td><td align='right'>%.3f</td></tr>\n" % (future_pipe_size))
    html.write("<tr><td>Raw</td><td align='right'>" + str(future_raw_count) + "</td><td align='right'>%.3f</td></tr>\n" % (future_raw_size))

    html.write("</table>")
    html.write("</td><td>")
    html.write("<img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/data_size_on_tape.png\">\n")
    html.write("</td></tr></table>\n")

def historical(html, days):
    """ Method to print out historical trands

        Parameters
        ----------
        html : file handle
        days : int
            The number of days included in report
    """
    html.write("<p><b>Historical Trends Last %s Days</b>" % (days))

    html.write("<p><img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/transfer.png\"><p><img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/transfer-size.png\"><P>\n")
    html.write("<img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/pprocess-count.png\"><p><img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/rprocess-count.png\"><p>\n")
    html.write("<img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/transfer-rate.png\"><p>\n")
    html.write("<P>\n")

def get_db_holdings(cur, db_name, length):
    """ Method
    """
    if db_name:
        db_name = '-' + db_name
    cur.execute("select sum(tar_size) from backup_tape where file_type='DB%s' and created_date<add_months(SYSDATE, -%i)" % (db_name, length))
    results = cur.fetchall()
    if results:
        return results[0][0]/math.pow(1024., 4)
    return 0.0

def db_status(cur, html, db_proc, db_to_proc, length):
    """ Method to report the status of DB backups

        Parameters
        ----------
        cur : cursor object
        html : file handle
        db_proc : list
            List of processed directories
        db_to_proc : list
            List of directories to be processed
    """
    html.write("<b>Status of Database Backups</b></p>")

    tmp = df('/des008/db_backup')

    tmp2 = float(get_size('/des008/db_backup'))

    labels = ['Used', 'Free']
    sizes = [tmp2, (1024. * tmp[0]) - (tmp2)]
    colors = ['yellowgreen', 'gold']

    bu.Pie('/work/QA/technical/backups/des008_db_usage.png', sizes, labels, figsize=(5, 4),
           colors=colors, title='des008 usage').generate()

    html.write("<table border=0>\n<tr><th>Transferred</th><th>In Queue</th><th></th><th>Data older then %i months</th></tr><tr><td valign='top'>\n" % (length))
    html.write("<table border=1>\n")
    count = 0
    for dbp in db_proc:
        count += 1
        if count > 10:
            break
        html.write("<tr><td>%s</td></tr>\n" % (dbp.replace("/des008/db_backup/", "")))
    html.write("</table></td><td valign='top'>\n")
    html.write("<table border=1>\n")
    for dbp in db_to_proc:
        cur.execute("select filename from backup_db where path='%s' and status=1" % (dbp))
        results = cur.fetchall()
        rsize = 0
        for res in results:
            #print d
            rsize += os.path.getsize(os.path.join(dbp, res[0]))
        rsize /= math.pow(1024., 3)
        dirsize = float(get_size(dbp))/math.pow(1024., 3)
        clr = "\"#FFFFFF\""
        if dirsize > 1024.:
            clr = "\"#FFFF00\""
        if dirsize > 10240. or dirsize == 0.:
            clr = "\"#FF0000\""
        html.write("<tr><td>%s</td><td align='right' bgcolor=%s>{dsz:,.2f} G</td><td align='right'>{rsz:,.2f} G</td></tr>\n".format(dsz=dirsize, rsz=rsize) % (dbp.replace("/des008/db_backup/", ""), clr))
    html.write("</table></td>\n")
    html.write("<td><img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/des008_db_usage.png\">")
    html.write("</td><td valign='top'><table border=1><tr><th>Type</th><th>Size (Tb)</th></tr>\n")
    for sub in ['', 'sci', 'oper', 'test']:
        html.write("<tr><td>%s</td><td align='right'>%.3f</td></tr>\n" % (sub, get_db_holdings(cur, sub, length)))
    html.write("</table></tr></table>\n")

def main():
    """ Main entry point
    """
    util = None
    try:
        #html = open('/work/QA/technical/backups/index.html', 'w')
        html = open('/tmp/monitor.html', 'w')
        html.write("<html>\n<head>\n<title>Backup Status</title>\n</head>\n<body>\n")
        #now = datetime.datetime.now()
        #backup_time = now - datetime.timedelta(days=7)
        #str(backup_time)

        # get the command line arguments
        args = parse_options()
        if args['debug']:
            pprint.pprint(args)
            level = logging.DEBUG
        else:
            level = logging.INFO
        # initialize the logging and db connection
        now = datetime.datetime.now()
        util = bu.Util(args['des_services'], args['section'], "/local_big/backups/logs/monitor.log", "MONITOR", llevel=level)
        util.log(bu.Util.info, " Starting monitor scan")

        cur = util.cursor()

        # get the last date the script was run
        print 'a',datetime.datetime.now()
        (numxfer, xfersize, num_not_xfer, not_xfersize) = get_tape_data(cur)
        print 'b',datetime.datetime.now()
        (numproc, numtoproc, rawproc, rawtoproc, future_raw_size, future_pipe_size, raw_size, pipe_size, future_raw_count, future_pipe_count) = get_backupdir_data(cur, util.root)
        print 'c',datetime.datetime.now()
        (num_deprec, depsize) = get_deprecated(cur)
        print 'd',datetime.datetime.now()
        (dbcount, dbsize) = get_database(cur)
        print 'e',datetime.datetime.now()
        untrans = get_untransferred(cur)
        print 'f',datetime.datetime.now()
        (totaltarsize, rawsize, sizesbytype) = get_total_data(cur)

        bu.Pie('/work/QA/technical/backups/data_size_on_tape.png', sizesbytype.values(),
               sizesbytype.keys()).generate()

        html.write("<h2>Current Status as of " + str(now) + "</h2>\n")

        report_processes(html)

        report_archive_status(html, numxfer, xfersize, num_deprec, depsize, numproc, totaltarsize, dbcount, dbsize, rawproc, rawsize, not_xfersize, num_not_xfer, numtoproc, rawtoproc,
                              future_raw_size, future_pipe_size, raw_size, pipe_size, future_raw_count, future_pipe_count)

        cur.execute("insert into friedel.backup_monitor (number_transferred,number_not_transferred,size_transferred,size_to_be_transferred,number_deprecated,size_deprecated,pipe_processed,pipe_to_be_processed,raw_processed,raw_to_be_processed,run_time) values(%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'))" % (numxfer, num_not_xfer, xfersize, not_xfersize, num_deprec, depsize, numproc, numtoproc, rawproc, rawtoproc, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        cur.execute('commit')

        report_untransferred(html, untrans)

        html.write('</td><td>\n')
        tmp = df('/local_big')

        tmp2 = float(get_size('/local_big/backups/staging'))
        tmp3 = float(get_size('/local_big/backups/transfer'))

        labels = ['Staging', 'Transfer', 'Free']
        sizes = [tmp2, tmp3, (1024. * tmp[0]) - (tmp2 + tmp3)]
        colors = ['yellowgreen', 'gold', 'lightskyblue']

        bu.Pie('/work/QA/technical/backups/local_big_status.png', sizes, labels, figsize=(5, 4),
               colors=colors, title='Data Backup (disk usage)').generate()

        html.write("<img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/local_big_status.png\">\n")
        html.write("</td><td>\n")
        tmp = df('/local_med')

        tmp2 = float(get_size('/local_med/db_backups/staging'))
        tmp3 = float(get_size('/local_med/db_backups/transfer'))

        labels = ['Staging', 'Transfer', 'Free']
        sizes = [tmp2, tmp3, (1024. * tmp[0]) - (tmp2 + tmp3)]

        bu.Pie('/work/QA/technical/backups/local_med_status.png', sizes, labels, figsize=(5, 4),
               colors=colors, title='DB Backup (disk usage)').generate()
        html.write("<img src=\"https://desar2.cosmology.illinois.edu/DESFiles/desardata/QA/technical/backups/local_med_status.png\">\n")
        html.write("</td></tr></table>")

        cur.execute("select number_transferred,number_not_transferred,size_transferred,size_to_be_transferred,number_deprecated,size_deprecated,pipe_processed,pipe_to_be_processed,raw_processed,raw_to_be_processed,run_time from friedel.backup_monitor order by run_time desc")

        results = cur.fetchall()
        tempx = []
        tempxf = []
        tempnxf = []
        tempsxf = []
        tempsnxf = []
        #tempnd = []
        #tempsd = []
        temppp = []
        tempptp = []
        temprp = []
        temprtp = []
        for i in results:
            tempx.append(i[10])      # run_time
            tempxf.append(i[0])      # number transferred
            tempnxf.append(i[1])     # number_not_transferred
            tempsxf.append(i[2])     # size_transferred
            tempsnxf.append(i[3])    # size_to_be_transferred
            #tempnd.append(i[4])      # number_deprecated
            #tempsd.append(i[5])      # size_deprecated
            temppp.append(i[6])      # pipe_processed
            tempptp.append(i[7])     # pipe_to_be_processed
            temprp.append(i[8])      # raw_processed
            temprtp.append(i[9])     # raw_to_be_processed
        runtime = np.array(tempx)
        numxfer = np.array(tempxf)
        num_not_xfer = np.array(tempnxf)
        trans_size = np.array(tempsxf)
        trans_size /= math.pow(1024, 4)
        size_to_trans = np.array(tempsnxf)
        size_to_trans /= math.pow(1024, 4)
        #npnd = np.array(nd)
        #npsd = np.array(sd)
        num_pipe_proc = np.array(temppp)
        num_pipe_to_proc = np.array(tempptp)
        num_raw_proc = np.array(temprp)
        num_raw_to_proc = np.array(temprtp)

        #cur.execute("select transfer_date,(tar_size/(1024*1024*1024)),(tar_size/(transfer_time*1024*1024*1024)) from backup_tape where transfer_date is not null and transfer_date >= TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS') order by transfer_date desc" % (start.strftime('%Y-%m-%d %H:%M:%S')))
        cur.execute("select transfer_date,(tar_size/(1024*1024*1024)),(tar_size/(transfer_time*1024*1024*1024)) from backup_tape where transfer_date is not null order by transfer_date desc")
        res = cur.fetchall()

        transdate = []
        transsize = []
        transtime = []
        for i in res:
            transdate.append(i[0])
            transsize.append(i[1])
            transtime.append(i[2])

        util.log(bu.Util.info, " Scan complete.")

        cur.execute("select distinct(path),run_date from backup_db where status=0 order by run_date desc")

        results = cur.fetchall()
        db_to_proc = []
        for i in results:
            db_to_proc.append(i[0])
        db_to_proc.sort()
        db_to_proc.reverse()
        cur.execute("select distinct(path),run_date from backup_db where status=1 order by run_date desc")
        results = cur.fetchall()
        db_proc = []
        for i in results:
            db_proc.append(i[0])
        #db3 = []
        #for i in results:
        #    db3.append(i[0])

        #db_proc = [q for q in db3 if q not in db_to_proc]

        # make the plots
        fig1 = bu.BoxPlot('/work/QA/technical/backups/transfer-rate.png', transdate[:14*8], 'Date',
                          'Rate (Gb/Hour) or Size (Gb)', 'Transfer Rates', xdate=True, dodots=True)
        fig1.add_ydata(transsize[:14*8], 'File Size (Gb)')
        fig1.add_ydata(transtime[:14*8], 'Transfer Rate (Gb/Hr)')
        fig1.generate()

        fig2 = bu.BoxPlot('/work/QA/technical/backups/pprocess-count.png', runtime, 'Date',
                          'Number of Runs', "Pipeline Runs' Progress", xdate=True)
        fig2.add_ydata(num_pipe_to_proc, 'In queue')
        fig2.add_ydata(num_pipe_proc, 'Tarred')
        fig2.add_ydata(num_pipe_proc + num_pipe_to_proc, 'Total')
        fig2.generate()

        fig3 = bu.BoxPlot('/work/QA/technical/backups/transfer.png', runtime, 'Date',
                          'Number of Tape Tar Files', 'Tape Tar File Transfer Status', xdate=True)

        fig3.add_ydata(num_not_xfer, 'In queue')
        fig3.add_ydata(numxfer, 'Transferred')
        fig3.add_ydata(numxfer + num_not_xfer, 'Total')
        fig3.generate()

        fig4 = bu.BoxPlot('/work/QA/technical/backups/transfer-size.png', runtime, 'Date', 'Tbytes',
                          'Backup Size (on tape)', xdate=True)
        fig4.add_ydata(size_to_trans, 'In queue')
        fig4.add_ydata(trans_size, 'Transferred')
        fig4.add_ydata(trans_size + size_to_trans, 'Total')
        fig4.generate()

        fig5 = bu.BoxPlot('/work/QA/technical/backups/rprocess-count.png', runtime, 'Date',
                          'Number of Nites', 'DTS Status', xdate=True)
        fig5.add_ydata(num_raw_to_proc, 'In queue')
        fig5.add_ydata(num_raw_proc, 'Tarred')
        fig5.add_ydata(num_raw_proc + num_raw_to_proc, 'Total')
        fig5.generate()

        historical(html, args['dlen'])
        db_status(cur, html, db_proc, db_to_proc, args['dblen'])
        html.write("</body>\n</html>\n")
        html.close()
        util.close()
        shutil.move('/tmp/monitor.html', '/work/QA/technical/backups/index.html')
    except Exception, ex:
        if util is not None:
            util.log(bu.Util.error, "Exception: " + str(ex))
            #if util.conn != None:
            #    util.conn.rollback()
            #    util.conn.close()
        raise


if __name__ == "__main__":
    main()
