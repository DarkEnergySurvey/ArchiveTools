#!/usr/bin/env python
""" Module to back up database file to tape
"""

import os
import pprint
import shutil
import datetime
import argparse
import gzip

import archivetools.backup_util as bu
from archivetools.DES_tarball import DES_tarball


def parse_options():
    """ Method to get the command line arguments

        Returns
        -------
        Tuple containing the options ar args
    """
    parser = argparse.ArgumentParser(description='Backup DB files')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode',)
    parser.add_argument('--max-pri', default="5",
                        help="Maximum priority level to process DEFAULT:%default",)
    parser.add_argument('--archive_size', '-s', default="300g",
                        help='Archive Tarball Minimum Size DEFAULT:%default',)
    parser.add_argument('--free', default="3t",
                        help='Minimum free space to have on disk. DEFAULT:%default',)
    parser.add_argument('--stgdir', default="/local_med/db_backups/staging",
                        help='DESAR Staging Directory DEFAULT:%default',)
    parser.add_argument('--xferdir', default="/local_med/db_backups/transfer",
                        help='DESAR transfer directory DEFAULT:%default',)
    parser.add_argument('--forcex', default=False, action='store_true',
                        help='Force the transfer of data, even if the minimum size is not met. DEFAULT:%default')
    parser.add_argument('--des_services', action='store',
                        help='DESDM Database Access File: %default',)
    parser.add_argument('--section', action='store',
                        help='Database to use',)
    parser.add_argument('--verify', default=False, action='store_true',
                        help='Verify file md5sums, this can take a while')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Turn on verbose mode. Default: %default',)
    return vars(parser.parse_args())


def archive_files(util, args):
    """ Method to archive DB files

        Parameters
        ----------
        util : Util object
        args : dict
            The command line arguments
    """
    util.log(bu.Util.info, "Starting DB backup")
    now = datetime.datetime.now()
    archive = None
    maximum_archive_size = bu.calculate_archive_size(args['archive_size'])
    cur = util.cursor()
    start = datetime.datetime.now() - datetime.timedelta(days=7)
    cur.execute("select distinct(PATH),run_date from BACKUP_DB where STATUS=0 and run_date <= TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS') order by run_date" % (start.strftime('%Y-%m-%d %H:%M:%S')))

    dirs = cur.fetchall()
    count = len(dirs)
    if count > 0:
        for dirn in dirs:
            dirname = dirn[0]
            cur.execute("select filename,run_date from backup_DB where path='%s' and status=0" % (dirname))
            subname = ""
            if "desoper" in dirname:
                subname = "-oper"
            elif "dessci" in dirname:
                subname = "-sci"
            elif "destest" in dirname:
                subname = "-test"
            util.log(bu.Util.info, "looking for " + dirname)
            fls = cur.fetchall()
            files = []
            sizes = 0
            fnum = 0
            data = {}
            for i, fname in enumerate(fls):
                if not util.ping():
                    util.reconnect()
                    cur = util.cursor()

                rundate = fname[1]
                util.log(bu.Util.info, "zipping " + fname[0])
                fname = fname[0]
                fpath = os.path.join(dirname, fname)
                zipfile = fname + ".gz"
                with open(fpath, 'rb') as f_in, gzip.open(os.path.join(args['stgdir'], zipfile), 'wb', compresslevel=1) as f_out:
                    shutil.copyfileobj(f_in, f_out)
                size = os.path.getsize(os.path.join(args['stgdir'], zipfile))
                files.append(fname + '.gz')
                if args['verify']:
                    data[fname] = [None, bu.generate_md5sum(fname)]
                sizes += size
                if sizes >= maximum_archive_size or i == len(fls) - 1:
                    util.log(bu.Util.info, "Working bin")
                    #archive = DES_archive(options, util, 'DB', 0)
                    flist = []
                    #print type(rundate)
                    cur.execute("select name from prod.backup_unit where name like '%%DB_BACKUP2_%s%s%%'" % (str(rundate.date()), subname))
                    results = cur.fetchall()
                    print fnum, len(results), str(rundate.date()), subname
                    if fnum == 0 and len(results) != 0:
                        for res in results:
                            fnum = max(int(res[0].split(".")[1]), fnum)
                    fnum += 1
                    #archive.setup_for_db("DB_BACKUP2_%s%s.%i" % (str(rundate.date()), subname, fnum))
                    archive = DES_tarball(args, files, data, util, args['stgdir'], file_class='DB%s' % subname, verify=args['verify'], tarname="DB_BACKUP2_%s%s.%i.tar" % (str(rundate.date()), subname, fnum))

                    for fln in files:
                        #archive.add_file(os.path.join(options.stgdir, fln + ".gz"))
                        flist.append((fln.replace('.gz',''),))
                        print fln
                        os.remove(os.path.join(args['stgdir'], fln))

                    md5sum = archive.get_md5sum()
                    tsize = archive.get_filesize()
                    util.log(bu.Util.info, "DB work")
                    print "insert into BACKUP_UNIT (NAME,DEPRECATED,TAR_SIZE,MD5SUM,CREATED_DATE,FILE_TYPE,STATUS) values ('%s',0,%i,'%s',TO\
_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),'%s',2)" % (archive.tarfile, tsize, md5sum, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'DB%s' % subname)
                    cur.execute("insert into BACKUP_UNIT (NAME,DEPRECATED,TAR_SIZE,MD5SUM,CREATED_DATE,FILE_TYPE,STATUS) values ('%s',0,%i,'%s',TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),'%s',2)" % (archive.tarfile, tsize, md5sum, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'DB%s' % subname))
                    qry = "update BACKUP_DB set UNIT_NAME='%s',STATUS=1 where PATH='%s' and filename=:1" % (archive.tarfile, dirname)
                    cur.prepare(qry)
                    cur.executemany(None, flist)
                    shutil.move(os.path.join(args['stgdir'], archive.tarfile), os.path.join(args['xferdir'], archive.tarfile))
                    cur.execute("update backup_unit set status=1,tape_tar='%s' where name='%s'" % (archive.tarfile, archive.tarfile))
                    sql = "insert into BACKUP_TAPE (NAME,TAR_SIZE,CREATED_DATE,MD5SUM,RETRIES,STATUS,PATH,DEPRECATED,PRIORITY,FILE_TYPE) values ('%s',%i,TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),'%s',0,0,'%s',0,%i, '%s')" % (archive.tarfile, tsize, now.strftime('%Y-%m-%d %H:%M:%S'), md5sum, args['xferdir'], 1, 'DB%s' % subname)
                    print sql
                    cur.execute(sql)
                    cur.execute('commit')
                    #util.reconnect()
                    #cur = util.conn.cursor()
                    files = []
                    sizes = 0

def main():
    """ Main entry
    """
    args = parse_options()
    if args['debug']:
        pprint.pprint(args)
    util = bu.Util(args['des_services'], args['section'], "/local_big/backups/logs/backup_db.log", "BACKUPDB", reqfree=bu.calculate_archive_size(args['free']))
    #util.connect(options.desdm, options.db)
    #freespace = calc_free_space(options.stgdir)
    #reqfree = calculate_archive_size(options.free)
    #if freespace < reqfree:
    #    print freespace,options.stgdir,reqfree
    #    Util.log(Util.error,"Not enough free space: %i on %s need %i." % (freespace,options.stgdir,reqfree))
    #if not Util.checkfreespace(options.stgdir):
    #    sys.exit()
    #else:
    #    Util.log(Util.info,"Free space: %i / %i" % (freespace,reqfree))
    try:
        #print "starting archive"
        archive_files(util, args)
    except Exception, ex:
        util.log(bu.Util.error, "Exception raised: " + str(ex))
        raise

if __name__ == "__main__":
    main()
