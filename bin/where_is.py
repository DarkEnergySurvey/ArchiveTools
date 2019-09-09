#!/usr/bin/env python
""" Method to find where a specific file is in the tape archive
"""

import pprint
import argparse

#import archivetools.backup_util as bu
from archivetools.backup_util import Util, locate


def parse_options():
    """ Method to parse command line options

        Returns
        -------
        Tuple containing the options and arguments
    """
    parser = argparse.ArgumentParser(description='Find a file in the tape archive')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Toggle DEBUG mode')
    parser.add_argument('--des_services', action='store', help='services file to use')
    parser.add_argument('--section', '-s', action='store', help='section of services file to use')
    parser.add_argument('--filename', '-f', action='store', help="The file to look for.")
    parser.add_argument('--reqnum', '-r', action='store', help="The request number to search for.")
    parser.add_argument('--unitname', '-u', action='store', help="The unit name to search for.")
    parser.add_argument('--attnum', '-a', action='store', help="The attempt number to search for")
    parser.add_argument('--pfwid', '-p', action='store', help='The pfw_attempt_id to find')
    parser.add_argument('--archive', action='store', default='desar2home', help='Name of the archive')
    parser.add_argument('--path', action='store', help='path to locate')
    args = parser.parse_args()
    return vars(args)

def main():
    """ Main entry
    """
    args = parse_options()
    if args['debug']:
        pprint.pprint(args)
    util = Util(args['des_services'], args['section'])
    #util.connect(args['des_services'], args['section'])
    try:
        data = locate(util, args['filename'], args['reqnum'], args['unitname'],
                         args['attnum'], args['pfwid'], args['path'], args['archive'])
        if data['unit'] and not data['tape']:
            print "Item is located in\n  Unit Tar: %s  created on %s\n and has not been added to a Tape Tar yet." % (data['unit'], data['unitdate'].strftime("%Y-%m-%d"))
        if data['tape']:
            if data['transdate'] is None:
                print "Item is located in\n  Unit Tar: %s  created on %s\n  Tape Tar: %s  created on %s\nand has not been transferred yet." % (data['unit'], data['unitdate'].strftime("%Y-%m-%d"), data['tape'], data['tapedate'].strftime("%Y-%m-%d"))
            else:
                print "Item is located in\n  Unit Tar: %s  created on %s\n  Tape Tar: %s  created on %s  transferred on %s" % (data['unit'], data['unitdate'].strftime("%Y-%m-%d"), data['tape'], data['tapedate'].strftime("%Y-%m-%d"), data['transdate'].strftime("%Y-%m-%d"))
    except ValueError:
        raise Exception("No input file or id's given, please specify one via -f or use --reqnum, --unitname, and --attnum or --pfwid")

if __name__ == "__main__":
    main()
