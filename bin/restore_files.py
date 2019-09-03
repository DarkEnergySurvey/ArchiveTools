#!/usr/bin/env python
""" Module to restore files from tape

"""

import os
import argparse
import sys
import tarfile
import re
from XRootD import client

import archivetools.backup_util as bu
import despymisc.miscutils as miscutils
import filemgmt.compare_utils as cu

def get_tape(filename):
    """ Method to get the given tape file from Fermi

        Parameters
        ----------
        filename : str
            The name of the file to retrieve
    """
    #if rtn != 0:
    if not status[1].ok:
        print 'Error retrieving tape file from archive.'
        sys.exit(1)

def restore_files(util, args, data):
    """ Method to restore file to the file system

        Parameters
        ----------
        util : Util instance
        args : dict
            Command line arguments
        data : dict
            Data on the files to restore
    """
    tape_tar = tarfile.open(args['tape'], mode='r')
    names = tape_tar.getnames()
    if args['unit'] not in names:
        raise Exception('Unit tar %s not found in tape tar %s, this should not happen' % (args['unit'], args['tape']))
    unit_tar = tarfile.open(fileobj=tape_tar.extractfile(args['unit']))

    root_path = '.'
    if args['restore']:
        root_path = data['archive']
    else:
        args['update_fai'] = False

    if args['filename'] or args['path']:
        if args['filename']:
            regex = re.compile(r'%s\Z' % (args['filename']))
        else:
            regex = re.compile(r'\A%s' % (args['path']))
        allnames = [m for m in unit_tar.getnames() if regex.search(m)]
        unit_tar.extractall(path=root_path, members=[m for m in unit_tar.getmembers() if regex.search(m.name)])
    else:
        unit_tar.extractall(path=root_path)
        allnames = unit_tar.getnames()
    if args['update_fai']:
        # get only the file names
        files = [m for m in allnames if unit_tar.getmember(m).isfile()]
        full_listing = {}
        for fln in files:
            full_filename = fln.split('/')[-1]
            direct = fln.replace('/' + full_filename, '')
            (filename, compression) = miscutils.parse_fullname(full_filename, miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION)
            full_listing[full_filename] = {'filename': filename,
                                           'compression': compression,
                                           'path': direct,
                                           'desfile_id': None,
                                           'archive': args['archive']}
        gtt = util.load_gtt_filename(full_listing.values())
        cur = util.cursor()
        cur.execute('select df.id, df.filename, df.compression from desfile df, %s gtt where gtt.filename=df.filename and gtt.compression=df.compression' % (gtt))
        results = cur.fetchall()
        desfile_ids = []
        for res in results:
            full_listing[res[1] + res[2]]['desfile_id'] = res[0]
            desfile_ids.append(res[0])
        # find any files not resgistered in desfile
        bad_files = {}
        if len(desfile_ids) != len(full_listing):
            for key, value in full_listing.iteritems():
                if not value['desfile_id']:
                    bad_files[key] = value
            full_listing = {key:full_listing[key] for key in full_listing if key not in bad_files.keys()}

        #gttid = util.conn.load_id_gtt(desfile_ids)
        # get files which are alread in file_archive_info
        #cur.execute('select desfile_id from file_archive_info fai, %s gtt where gtt.id=fai.desfile_id' % (gttid))
        #results = cur.fetchall()
        #loaded_ids = []
        #for res in results:
        #    loaded_ids.append(res[0])

        cur.prepare("merge into file_archive_info fai using dual on (fai.desfile_id=:desfile_id) when matched then update set path=:path,archive_name=:archive when not matched then insert (filename, archive_name, path, compression, desfile_id) values (:filename, :archive, :path, :compression, :desfile_id)")

        cur.executemany(None, full_listing)
        util.commit()
        if bad_files:
            print "WARNING: The following files we not added to FILE_ARCHIVE_INFO because they do not have entries"
            print "in DESFILE. They will need to be manually ingested with register_files.py"
            for key, value in bad_files.iteritems():
                print os.path.join(value['path'], key)
            print ''
        # DO CHECK
    if args['verify'] and args['archive']:
        print "Starting integrity check of files..."
        comp_args = {'dbh': util,
                     'des_services': args['des_services'],
                     'section': args['section'],
                     'archive': args['archive'],
                     'md5sum': True,
                     'verbose': args['verbose'],
                     'silent': False}
        if args['pfwid']:
            comp_args['pfwid'] = args['pfwid']
        elif args['reqnum']:
            comp_args['reqnum'] = args['reqnum']
            comp_args['unitname'] = args['unitname']
            comp_args['attnum'] = args['attnum']
        else:
            if args['filename']:
                fullpath = allnames[0]
                args['path'] = fullpath[:fullpath.rfind('/')]
            comp_args['relpath'] = args['path']

        cu.compare(**comp_args)

def main():
    """ Main entry
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', action='store', default=None)
    parser.add_argument('--pfwid', action='store', default=None)
    parser.add_argument('--path', action='store', default=None)
    parser.add_argument('--reqnum', '-r', action='store', help="The request number to search for.")
    parser.add_argument('--unitname', '-u', action='store', help="The unit name to search for.")
    parser.add_argument('--attnum', '-a', action='store', help="The attempt number to search for")
    parser.add_argument('--archive', action='store', default='desar2home')
    parser.add_argument('--section', action='store', default=None)
    parser.add_argument('--des_services', action='store', default=None)
    parser.add_argument('--verbose', '-v', action='store_true', default=False)
    parser.add_argument('--verify', action='store_true', default=False)
    parser.add_argument('--update_fai', action='store_true', default=False)
    parser.add_argument('--restore', action='store_true', default=False,
                        help='Restore the requested files to their proper place in the archive if possible. Default is the current directory.')
    args = parser.parse_args(sys.argv[1:])
    vars(args)
    # validate the args
    triplet = [args['reqnum'], args['unitname'], args['attnum']]
    if not any(triplet + [args['filename'], args['path']]):
        raise Exception('One of the filename, pfwid, path or (reqnum, unitname, attnum) options must be given.')

    if (args['filename'] and (any(triplet) or args['path'] or args['pfwid'])) or \
        (any(triplet) and (args['pfwid'] or args['path'])) or \
        (args['pfwid'] and args['path']):
        raise Exception('Only one of the filename, pfwid, path, or (reqnum, unitname, attnum) options can be given.')
    if any(triplet) and not all(triplet):
        raise Exception('The use of any of the reqnum, unitname, and attnum options requires the use of the others.')

    if (args['restore'] or args['verify'] or args['update_fai']) and not args['archive']:
        raise Exception('The restore, verify, and update_fai options require the use of the archive argument.')

    util = bu.Util(services=args['des_services'], section=args['section'])
    #util.connect()

    data = bu.locate(util, args['filename'], args['reqnum'], args['unitname'],
                     args['attnum'], args['pfwid'], args['path'], args['archive'])
    if not data['tape']:
        print "Item is not in tape archive"
        return
    get_tape(data['tape'])
    restore_files(util, args, data)
#    if args['filename']:
#        restore_file(args['filename'], data, args['restore'])
#    else:
#        restore_path(data, args['restore'])

if __name__ == "__main__":
    main()
