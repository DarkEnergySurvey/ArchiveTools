#!/usr/bin/env python2
import matplotlib
matplotlib.use('PS')
import unittest
from archivetools import backup_util as bu


class TestBadPermissions(unittest.TestCase):
    def test_blank(self):
        self.assertEqual(bu.get_subdir('RAW_FILE'), 'DTS')
        self.assertEqual(bu.get_subdir('DB_BACKUPSTUFF'), 'DB')
        self.assertEqual(bu.get_subdir('ANYTHING_ELSE'), 'OPS')
if __name__ == '__main__':
    unittest.main()
