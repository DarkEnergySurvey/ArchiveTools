#!/usr/bin/env python
"""
Module to determine which data need to be backup up to tape
"""

import datetime
import logging
import argparse
import os
import time
import glob
import pprint

from archivetools.backup_util import Util

SECINWEEK = 60*60*24*7


def parse_options():
    """ Parse any command line options

        Returns
        -------
        Dict containing the command line arguments
    """
    parser = argparse.ArgumentParser(description='Stage data to be archived to tape')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode',)
    parser.add_argument('--des_services', action='store', help='DESDM Database Access File: %default',)
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Turn on verbose mode. Default: %default',)
    parser.add_argument('--section', action='store', help='Database to use',)
    return vars(parser.parse_args())

def junk_runs(util):
    """ Method to remove any directories that have been marked as JUNK from the available backups list

        Parameters
        ----------
        util : Util object
            used for DB connection and logging
    """
    #con = util.get_connect(options.desdm, options.db)
    cur = util.cursor()

    cur.execute("select archive_path from prod.pfw_attempt where data_state='JUNK' and archive_path in (select path from prod.backup_dir where status=0)")
    dirs = cur.fetchall()
    util.log(Util.info, "  Dropping %i junked runs." % (len(dirs)))
    cur.prepare("delete from prod.backup_dir where path=:1")
    cur.executemany(None, dirs)
    util.commit()

def add_dirs(cur, util, dirlist):
    """ Method to get any new pipeline directories

        Parameters
        ----------
        cur : cursor object
        util : Util object
    """
    cur.execute("select a.archive_path,a.data_state,c.end_time,b.pipeline,b.campaign,c.status,a.id from prod.pfw_attempt a inner join prod.pfw_request b on a.reqnum = b.reqnum inner join prod.task c on a.task_id = c.id where c.end_time is not null and a.data_state <> 'JUNK' and a.archive_path like '%OPS%' and a.archive_path not like '%/hostname/%' and archive_path not in (select path from prod.backup_dir)")

    dirl = cur.fetchall()
    util.log(Util.info, "  Found %i pipeline files to process." % (len(dirl)))
    # archive_path, data_state, state_change_date, pipeline, campaign, status, pfwid
    #     0              1              2              3        4        5      6
    for adir in dirl:
        if adir[4] is None:
            continue
        path = adir[0]
        pfwid = adir[6]
        state = adir[1].upper()
        if "ACTIVE" in state:
            dtm = adir[2] + datetime.timedelta(days=7)  # was 1
        elif adir[5] == 0:
            dtm = adir[2] + datetime.timedelta(days=7)
        else:
            continue
        pipeline = adir[3].lower()
        camp = adir[4].upper()
        if ("finalcut" in pipeline or "coadd" in pipeline or "multiepoch" in pipeline):
            priority = 1
        elif "sne" in pipeline:
            if "REPROC" in camp:
                priority = 2
            elif "y1" not in camp and "y2" not in camp:
                priority = 4
        elif "firstcut" in pipeline or "supercal" in pipeline:
            priority = 3
        elif "precal" in pipeline or 'photoz' in pipeline or 'prebpm' in pipeline:
            priority = 4
        else:
            priority = 10
        dirlist.append((path, pipeline, dtm.strftime('%Y-%m-%d %H:%M:%S'), priority, pfwid))
    util.log(Util.info, "  Added %i pipeline files." % (len(dirlist)))

def get_all_raw(cur, sql, ftype, dirlist):
    """ Method to get raw/snmanifest directories

        Parameters
        ----------
        cur : db cursor
        sql : str
            sql statement to execute
        ftype : str
            the file type
        dirlist : list
            List of the directories
    """
    count = 0
    cur.execute(sql)

    nites = cur.fetchall()
    for nite in nites:
        date = nite[0]
        dtm = datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        if dtm > datetime.datetime(2014, 07, 01):
            dtm += datetime.timedelta(days=7)
            directory = os.path.join("DTS", ftype, date)
            dirlist.append((directory, "RAW", dtm.strftime('%Y-%m-%d %H:%M:%S'), 5, None))
            count += 1
    return count

def get_raw(cur, util, dirlist):
    """ Method to get any new raw directories

        Parameters
        ----------
        cur : cursor object
        util : Util object
        dirlist : list
            list of directories to add
    """
    sql = "select distinct nite from prod.exposure where 'DTS/raw/' || nite not in (select path from prod.backup_dir) and nite not like '%2013%' and nite not like '%2012%' and nite not like '%2016%'"

    util.log(Util.info, "  Found %i nites to process" % (get_all_raw(cur, sql, 'raw', dirlist)))

def get_sne(cur, util, dirlist):
    """ Method to get any new sne directories

        Parameters
        ----------
        cur : cursor object
        util : Util object
        dirlist : list
            list of directories to add
    """
    sql = "select distinct nite from prod.manifest_exposure where 'DTS/snmanifest/' || nite not in (select path from prod.backup_dir) and nite not like '%2013%' and nite not like '%2012%'"

    util.log(Util.info, "  Found %i sn nites to process" % (get_all_raw(cur, sql, 'snmanifest', dirlist)))

def get_db(cur, util):
    """ Method to add DB backup directories

        Parameters
        ----------
        cur : cursor object
        util : Util object
    """
    cutoff = time.time() - SECINWEEK
    dbfiles = []
    for directory in ["/des008/db_backup/desoper/snaps", "/des008/db_backup/dessci/snaps", "/des008/db_backup/destest/snaps"]:
        print directory
        ctime = time.time()
        for root, _, fnames in os.walk(directory):
            dbf = root[root.rfind("/") + 1:]
            try:
                int(dbf[2])
            except ValueError:
                continue
            if len(fnames) == 0:
                continue
            list_of_files = glob.glob('%s/*' % (root))
            # get last modified file time
            lmtime = os.path.getmtime(max(list_of_files, key=os.path.getctime))
            # stop if the file is less than 24 hours old
            if lmtime > (ctime - 86400.):
                continue
            cur.execute("select count(*) from backup_db where path='%s'" % (root))
            if cur.fetchall()[0][0] > 0:
                print "skipping ", root
                continue
            found = False
            for fnm in fnames:
                if os.path.getmtime(os.path.join(root, fnm)) > cutoff:
                    print "skipping2", os.path.join(root, fnm)
                    found = True
                    break
            if found:
                continue
            for fnm in fnames:
                print "ADDING", os.path.join(root, fnm)
                dbfiles.append((root, fnm, dbf))
    cur.prepare("insert into backup_db (path, filename, run_date) values (:1,:2,TO_DATE(:3, 'YYYYMMDD'))")
    cur.executemany(None, dbfiles)
    util.log(Util.info, "  Added %i database files" % (len(dbfiles)))

def main():
    """ Main code block
    """
    util = None
    # set the default priorities for the different data types (higher number is lower priority)
    #priority = {"finalcut":   1,
    #            "coadd":      1,
    #            "multiepoch": 1,
    #            "y2reproc":   2,
    #            "firstcut":   3,
    #            "supercal":   3,
    #            "precal":     4,
    #            "sne":        4,
    #            "prebpm":     4,
    #            "photoz":     4,
    #            "raw":        5}
    try:
        now = datetime.datetime.now()

        # get the command line arguments
        args = parse_options()
        if args['debug']:
            pprint.pprint(args)
            level = logging.DEBUG
        else:
            level = logging.INFO
        # initialize the logging and db connection
        util = Util(args['des_services'], args['section'], "/local_big/backups/logs/backup_setup.log", "SETUP", level)
        util.log(Util.info, " Starting backup scan at %s" % (now.strftime('%Y-%m-%d %H:%M:%S')))

        junk_runs(util)

        #conn = util.get_connect(options.desdm, options.db)
        cur = util.cursor()
        dirlist = []
        # first get pipeline stuff
        add_dirs(cur, util, dirlist)

        # now get the raw data
        get_raw(cur, util, dirlist)

        # SN data
        get_sne(cur, util, dirlist)

        # store what was found

        cur.prepare("insert into prod.backup_dir (path, status, class, release_date, priority, pfw_attempt_id) values (:1,0,:2,TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'),:4,:5)")
        cur.executemany(None, dirlist)

        # now scan for database files, this has to be done on disk
        get_db(cur, util)

        util.commit()
        cur.close()
        util.close()
        util.log(Util.info, " Scan complete.")
    except Exception, ex:
        if util is not None:
            #exc_type, exc_value, exc_traceback = sys.exc_info()
            util.log(Util.error, "Exception: " + str(ex).strip())  #,exc_traceback)
            util.rollback()
            util.close()
        raise


if __name__ == "__main__":
    main()
