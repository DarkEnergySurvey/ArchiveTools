#!/usr/bin/env python
""" Module to backup data directories to tape
"""

import os
import pprint
import sys
import datetime
import argparse

import archivetools.backup_util as bu
from archivetools.DES_archive import DES_archive


def parse_options():
    """ Method to parse command line options

        Returns
        -------
        Tuple of the options and args
    """
    parser = argparse.ArgumentParser(description='Run backups')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode')
    parser.add_argument('--max_pri', default="5",
                        help="Maximum priority level to process DEFAULT:%default")
    parser.add_argument('--archive_size', '-s', default="300g",
                        help='Archive Tarball Minimum Size DEFAULT:%default')
    parser.add_argument('--free', default="3t",
                        help='Minimum free space to have on disk. DEFAULT:%default')
    parser.add_argument('--stgdir', default="/local/Staging",
                        help='DESAR Staging Directory DEFAULT:%default')
    parser.add_argument('--xferdir', default="/local/Transfer",
                        help='DESAR transfer directory DEFAULT:%default')
    parser.add_argument('--forcex', default=False, action='store_true',
                        help='Force the transfer of data, even if the minimum size is not met. DEFAULT:%default')
    parser.add_argument('--des_services', action='store', help='DESDM Database Access File: %default',)
    parser.add_argument('--section', action='store', help='Database to use')
    parser.add_argument('--verify', default=False, action='store_true',
                        help='Verify file md5sums, this can take a while')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Turn on verbose mode. Default: %default')
    parser.add_argument('--class', action='store',
                        help='Select a specific class to process')
    return vars(parser.parse_args())

def archive_files(util, args):
    """ Method to archive data files

        Parameters
        ----------
        util : Util class
        args : dict
            Command line arguments
    """
    #now = datetime.datetime.now()
    level = 1
    archive = {}
    maximum_archive_size = bu.calculate_archive_size(args['archive_size'])
    while level <= int(args['max_pri']):
        if not util.ping():
            util.reconnect()
        cur = util.cursor()
        sql = "select PATH,CLASS from BACKUP_DIR where STATUS=0 and RELEASE_DATE <= TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS') " % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        if args['class']:
            sql += "and class='%s' " % (args['class'])
        else:
            sql += "and PRIORITY=%i" % (level)
        sql += "order by RELEASE_DATE DESC"
        cur.execute(sql)

        dirs = cur.fetchall()
        count = len(dirs)
        # force the processing if requested
        if count == 0 and args['forcex']:
            dirs = []
            if args['class']:
                dirs.append(([], args['class']))
            else:
                for i in bu.CLASSES:
                    dirs.append(([], i))
            count = len(dirs)
        if count > 0:
            classes = {}
            for ddir in dirs:
                if ddir[1] in classes:
                    classes[ddir[1]].append(ddir[0])
                else:
                    #print d[0],d[1]
                    if ddir[0]:
                        classes[ddir[1]] = [ddir[0]]
                    else:
                        classes[ddir[1]] = []
            for clss, fls in classes.iteritems():
                restart = False
                if clss in archive:
                    del archive[clss]
                archive[clss] = DES_archive(args, util, clss, level)
                if archive[clss].archive_size >= maximum_archive_size:
                    archive[clss].generate()
                    del archive[clss]
                    archive[clss] = DES_archive(args, util, clss, level)

                for path in fls:
                    restart = False
                    dfullpath = os.path.join(util.root, path)
                    util.log(bu.Util.info, " ==> Processing %s" % (dfullpath))
                    cur.execute("select t1.filename,t1.filesize,t1.md5sum,t2.compression from desfile t1, file_archive_info t2 where t1.filename = t2.filename and t2.path='%s' and ((t1.compression is null and t2.compression is null) or t1.compression = t2.compression)" % (path))
                        #cur.execute("select * from opm_artifact inner join file_archive_info on opm_artifact.NAME = file_archive_info.filename where file_archive_info.path='%s'" % (path))
                    listing = cur.fetchall()
                    data = {}
                    for lst in listing:
                        if lst[3] is not None:
                            data[lst[0] + lst[3]] = [lst[1], lst[2]]
                        else:
                            data[lst[0]] = [lst[1], lst[2]]
                    if args['verify']:
                        util.log(bu.Util.info, " ==> Checking md5sums of files")
                        #print Util.root
                        for dirpath, dirs, files in os.walk(os.path.join(util.root, path)):
                            error = False
                            for fname in files:
                                fullpath = os.path.join(dirpath, fname)
                                if fname in data:
                                    #a = time.time()
                                    md5 = bu.generate_md5sum(fullpath)
                                    #b = time.time()
                                    #total += b - a
                                    if md5 != data[fname][1]:
                                        util.log(bu.Util.error, "Incorrect md5sum in database for %s, it is listed as %s but is %s." % (fullpath, md5, data[fname][1]))
                                        # then we have an issue
                                        error = True
                                        break
                                else:
                                    size = os.path.getsize(fullpath)
                                    if size > 10*(1024**2):
                                        util.log(bu.Util.error, "Unexpected file too large to archive: %s" % (fullpath))
                                        error = True
                                        break
                                    else:
                                        data[fname] = [size, bu.generate_md5sum(fullpath)]
                            if error:
                                break
                        if error:
                            break
                        util.log(bu.Util.info, " ==> Check complete")
                    archive[clss].make_directory_tar(path, data, util.root)
                    util.log(bu.Util.info, " ==> Processing complete: %s" % (dfullpath))
                    if clss != 'RAW':
                        cur.execute("select pfw_attempt_id from backup_dir where path='%s'" % (path))
                        res = cur.fetchone()
                        if not res:
                            raise Exception("Could not find pfw_attempt_id for path %s" % (path))

                        pfwid = res[0]
                        cur.execute('select df.id,fai.path from desfile df, file_archive_info fai where df.id=fai.desfile_id and df.pfw_attempt_id=%i' % (pfwid))
                        afiles = cur.fetchall()
                        data = []
                        for fln in afiles:
                            data.append({'desfile_id': fln[0], 'spinning_archive_path': fln[1], 'tape_path': fln[1]})
                        try:
                            cur.prepare('insert into friedel.backup_path (desfile_id, spinning_archive_path, tape_path) values (:desfile_id,:spinning_archive_path,:tape_path)')
                            cur.executemany(None, data)
                            cur.execute('commit')
                        except:
                            print "Could not add path info to backup_path"
                            raise

                    if archive[clss].archive_size >= maximum_archive_size:
                        archive[clss].generate()
                        del archive[clss]
                        restart = True
                        archive[clss] = DES_archive(args, util, clss, level)
                    if not util.checkfreespace(args['stgdir']):
                        return
                    if restart:
                        break

                if restart:
                    level = 1
                    break
            level = 1
            if args['forcex']:
                for clss in classes.keys():
                    # force the transfer if needed
                    if archive[clss].archive_size > 0:
                        archive[clss].generate()
                        del archive[clss]
                break
            if args['class']:
                level = 100
        else:
            level += 1

def main():
    """ Main entry
    """
    args = parse_options()
    if args['debug']:
        pprint.pprint(args)
    util = bu.Util(args['des_services'], args['section'], "/local_big/backups/logs/backup.log", "BACKUP", reqfree=bu.calculate_archive_size(args['free']))
    #util.connect(options.desdm, options.db)
    if not util.checkfreespace(args['stgdir']):
        sys.exit()
    try:
        util.log(bu.Util.info, "Starting backup processing")
        archive_files(util, args)
    except Exception, ex:
        util.log(bu.Util.error, "Exception raised: " + str(ex))
        raise

if __name__ == "__main__":
    main()
