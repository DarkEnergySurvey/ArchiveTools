#!/usr/bin/env python
""" Module to transfer files to tape at Fermi
"""

import os
import time
import datetime
import pprint
import argparse
from XRootD import client

import archivetools.backup_util as bu

def parse_options():
    """ Method to parse command line options

        Returns
        -------
        Tuple containing the options and arguments
    """
    parser = argparse.ArgumentParser('Transfer files to Fermi tape')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode',)
    parser.add_argument('--project', '-p', default="DES",
                        help='Project Identifier',)
    parser.add_argument('--max-pri', default="5",
                        help="Maximum priority level to process DEFAULT:%default",)
    parser.add_argument('--dir', default="/archive_data/Archive",
                        help='Project Directory DEFAULT:%default',)
    parser.add_argument('--server', default="root://fndca4a.fnal.gov:1094",
                        help='Mass Storage Server DEFAULT:%default',)
    parser.add_argument('--mssdir', default="/pnfs/fnal.gov/usr/des/des_archive",
                        help='Mass Storage Archive Location DEFAULT:%default',)
    parser.add_argument('--stgdir', default="/local/Staging",
                        help='DESAR Staging Directory DEFAULT:%default',)
    parser.add_argument('--xfer_method', default="xrootd",
                        help='Mass Storage Transfer Mechanism: %default',)
    parser.add_argument('--noftp', default=False, action='store_true',
                        help='Skip transfer to mass storage',)
    parser.add_argument('--des_services',
                        help='DESDM Database Access File: %default',)
    parser.add_argument('--section', action='store',
                        help='Database to use',)
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Turn on verbose mode. Default: %default',)
    parser.add_argument('--max_tries', default='5', action='store',
                        help='The maximum number of tries to do before giving up.')
    return vars(parser.parse_args())

class Transfer(object):
    """ Class for transferring files

        Parameters
        ----------
        util : Util instance
        args : dict
            Command line arguments
    """
    def __init__(self, util, args):
        self.args = args
        self.util = util
        self.mss_dir = args['mssdir']
        self.server = args['server']
        self.tries = 0

    def transfer(self):
        """ Method to do the transfer
        """
        level = 1
        self.tries += 1
        while level <= int(self.args['max_pri']):
            print level, int(self.args['max_pri'])
            if not self.util.ping():
                self.util.reconnect()

            cur = self.util.cursor()
            cur.execute("select NAME,PATH,TAR_SIZE from PROD.BACKUP_TAPE where STATUS=0 and PRIORITY=%i" % (level))
            files = cur.fetchall()
            if len(files) > 0:
                for fln in files:
                    archive_name = os.path.join(fln[1], fln[0])
                    subdir = bu.get_subdir(fln[0])

                    if self.args['xfer_method'] == 'xrootd':
                        url = os.path.join(self.mss_dir, subdir, fln[0])
                        xrc = client.FileSystem(self.server)
                        self.util.log(bu.Util.info, "=> Transfering via {0}: ".format(self.args['xfer_method']))
                        self.util.log(bu.Util.info, "=> {0} ".format(self.server + url))
                        time1 = time.time()
                        status, info = xrc.copy(source=archive_name, target=self.server + url)
                        if not status.ok:
                            raise Exception('Transfer error: ' + status.message)
                        time2 = time.time()
                        time_to_transfer = (time2 - time1)/3600

                        status, info = xrc.stat(url)
                        if not status.ok:
                            raise Exception('Status error: ' + status.message)
                        if info.size < fln[2]:
                            self.util.log(bu.Util.error, "Incomplete transfer")
                            print "Incomplete transfer %i" % (info.size)
                            raise SystemExit

                    else:
                        srm_url = os.path.join(self.mss_dir, subdir, fln[0])

                        if self.args['xfer_method'] == 'rsync':
                            cmd = "rsync -xavP {0} mss:{1}".format(archive_name, self.mss_dir)
                        elif self.args['xfer_method'] == 'srmcp':
                            XXXXX
                        elif self.args['xfer_method'] == 'ftp':
                            print "Using ftp"
                        elif self.args['xfer_method'] == 'gridftp':
                            cmd = "globus-url-copy -rst -rst-retries 3 {0} {1}//{2}".format(archive_name, self.server, self.mss_dir)
                        elif self.args['xfer_method'] == 'gridftpdebug':
                            cmd = "globus-url-copy -rst -rst-retries 3 -vb -dbg {0} {1}//{2}".format(archive_name, self.server, self.mss_dir)
                        elif self.args['xfer_method'] == 'scp':
                            print "Using scp"
                        else:
                            print "Unknown transfer method"
                            self.util.log(bu.Util.error, "Unknown transfer method")
                            raise SystemExit

                        self.util.log(bu.Util.info, "=> Transfering via {0}: ".format(self.args['xfer_method']))
                        self.util.log(bu.Util.info, "=> {0} ".format(cmd))
                        time1 = time.time()
                        rec = os.system(cmd)
                        time2 = time.time()
                        time_to_transfer = (time2 - time1)/3600

                        if rec >= 1:
                            self.util.log(bu.Util.error, "Transfer failed: %i" % (rec))
                            print "Bad transfer: %i" % (rec)
                            raise SystemExit
                        if self.args['xfer_method'] == 'srmcp':
                            time.sleep(120)
                            bu.srmls(self.server, self.mss_dir, subdir, fln[0], int(fln[2]), self.util)
                            print "Transfer successful"

                    self.util.log(bu.Util.info, "=> Removing: {0}".format(archive_name))
                    os.remove(archive_name)
                    if not self.util.ping():
                        self.util.reconnect()
                    cur = self.util.cursor()
                    sql = "update PROD.BACKUP_TAPE set STATUS=1,TRANSFER_DATE=TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS'),TRANSFER_TIME=%f,RETRIES=%i where NAME='%s'" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), time_to_transfer, self.tries-1, fln[0])
                    print sql
                    cur.execute(sql)
                    #self.util.commit()
                    cur.execute('commit')
                    level = 1
            else:
                level += 1

    def get_tries(self):
        """ Method to return the number of tries

            Returns
            -------
            int of the number of tries
        """
        return self.tries

def main():
    """ Main entry
    """
    args = parse_options()
    if args['debug']:
        pprint.pprint(args)
    util = bu.Util(args['des_services'], args['section'], "/local_big/backups/logs/transfer.log", "TRANSFER")
    #util.connect(options.desdm, options.db)
    util.log(bu.Util.info, "Starting transfer at %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    xfer = Transfer(util, args)
    while xfer.get_tries() <= int(args['max_tries']):
        try:
            print 'start'
            xfer.transfer()
            print 'next'
            util.close()
            return
        except SystemExit:
            util.close()
            raise
        except Exception, ex:
            print 'Exception raised: ' + str(ex)
    raise Exception("Exceeded maximum number of tries")

if __name__ == "__main__":
    main()
