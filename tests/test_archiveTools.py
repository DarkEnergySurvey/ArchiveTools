#!/usr/bin/env python2
import matplotlib
matplotlib.use('PS')
import unittest
from contextlib import contextmanager
from StringIO import StringIO
from mock import patch, mock_open
from archivetools import backup_util as bu
import sys
import copy
import datetime
sys.path.append('bin')
sys.path.append('tests')

import where_is as wis

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestBackupUtil(unittest.TestCase):
    def test_get_subdir(self):
        # test each of the file groupings
        self.assertEqual(bu.get_subdir('RAW_FILE'), 'DTS')
        self.assertEqual(bu.get_subdir('DB_BACKUPSTUFF'), 'DB')
        self.assertEqual(bu.get_subdir('ANYTHING_ELSE'), 'OPS')
    
    def test_locate(self):
        self.assertTrue(True)

    def test_generate_md5sum(self):
        # test on a pre-generated file
        self.assertEqual(bu.generate_md5sum('tests/test.file'), '9a6944ab3ae1ab7843629a8e4d167bfb')
        
    def test_calculate_archive_size(self):
        b = 5
        kb = 3
        mb = 7
        gb = 2
        tb = 9
        pb = 8
        byte = 1 * b
        kbyte = 1024 * kb
        mbyte = 1048576 * mb
        gbyte = 1073741824 * gb
        tbyte = 1099511627776 * tb
        pbyte = 1125899906842624 * pb

        # test if input is in bytes
        self.assertEqual(bu.calculate_archive_size('%ib' % b), byte)
        self.assertEqual(bu.calculate_archive_size('%iB' % b), byte)
        # test if input is in kilobytes
        self.assertEqual(bu.calculate_archive_size('%ik' % kb), kbyte)
        self.assertEqual(bu.calculate_archive_size('%iK' % kb), kbyte)
        # test is input is in megabytes
        self.assertEqual(bu.calculate_archive_size('%im' % mb), mbyte)
        self.assertEqual(bu.calculate_archive_size('%iM' % mb), mbyte)
        # test if input is in gigabytes
        self.assertEqual(bu.calculate_archive_size('%ig' % gb), gbyte)
        self.assertEqual(bu.calculate_archive_size('%iG' % gb), gbyte)
        # test if input is in terabytes
        self.assertEqual(bu.calculate_archive_size('%it' % tb), tbyte)
        self.assertEqual(bu.calculate_archive_size('%iT' % tb), tbyte)
        # test if input is in petabytes
        self.assertEqual(bu.calculate_archive_size('%ip' % pb), pbyte)
        self.assertEqual(bu.calculate_archive_size('%iP' % pb), pbyte)
        # test for unsupported size
        self.assertEqual(bu.calculate_archive_size('%ix' % b), byte)
    
    def test_srmls(self):
        # test for blank file
        with patch('archivetools.backup_util.Util') as Ut:
            with patch('archivetools.backup_util.os.system') as syspatch:
                with patch('archivetools.backup_util.open', mock_open(read_data='')) as m:
                    with capture_output() as (out,err):
                        with self.assertRaises(SystemExit):
                            bu.srmls('a', 'b', 'c', 'd', 0, Ut)
                        output = out.getvalue().strip()
                        self.assertTrue('No such file on tape' in output)

        # test with blank file and Util
        with patch('archivetools.backup_util.os.system') as syspatch:
            with patch('archivetools.backup_util.open', mock_open(read_data='')) as m:
                with capture_output() as (out,err):
                    with self.assertRaises(SystemExit):
                        bu.srmls('a', 'b', 'c', 'd', 0, None)
                    output = out.getvalue().strip()
                    self.assertTrue('No such file on tape' in output)

        # test with correct file size
        with patch('archivetools.backup_util.os.system') as syspatch:
            with patch('archivetools.backup_util.open', mock_open(read_data='12345 file size')) as m:
                self.assertEqual(bu.srmls('a', 'b', 'c', 'd', 12345, None), 12345)

        # test with too small a file size and Util
        with patch('archivetools.backup_util.os.system') as syspatch:
            with patch('archivetools.backup_util.open', mock_open(read_data='12345 file size')) as m:
                with capture_output() as (out,err):
                    with self.assertRaises(SystemExit):
                        bu.srmls('a', 'b', 'c', 'd', 12346, Ut)
                    output = out.getvalue().strip()
                    self.assertTrue('12346' in output)
                    self.assertTrue('Incomplete transfer' in output)

        # test with too small a file size
        with patch('archivetools.backup_util.os.system') as syspatch:
            with patch('archivetools.backup_util.open', mock_open(read_data='12345 file size')) as m:
                with capture_output() as (out,err):
                    with self.assertRaises(SystemExit):
                        bu.srmls('a', 'b', 'c', 'd', 12346, None)
                    output = out.getvalue().strip()
                    self.assertTrue('12346' in output)
                    self.assertTrue('Incomplete transfer' in output)


    def test_check_files(self):
        self.assertTrue(True)
        
    def test_Util_init(self):
        self.assertTrue(True)
        
    def test_Util_ping(self):
        self.assertTrue(True)
        
    def test_Util_reconnect(self) :
        self.assertTrue(True)
       
    def test_Util_init_logger(self) :
        self.assertTrue(True)
        
    def test_Util_notify(self) :
        self.assertTrue(True)
        
    def test_Util_log(self) :
        self.assertTrue(True)
        
    def test_Util_checkfreespace(self) :
        self.assertTrue(True)
        
    def test_Plot_init(self):
        self.assertTrue(True)
        
    def test_Plot_save(self):
        self.assertTrue(True)
        
    def test_Plot_generate(self):
        self.assertTrue(True)
        
    def test_Pie_init(self) :
        self.assertTrue(True)
        
    def test_Pie_generate(self):
        self.assertTrue(True)
        
    def test_BoxPlot_init(self) :
        self.assertTrue(True)
        
    def test_BoxPlot_add_ydata(self):
        self.assertTrue(True)
        
    def test_BoxPlot_generate(self):
        self.assertTrue(True)
        

class TestDES_tarball(unittest.TestCase) :
    def test_DES_tarball_init(self):
        self.assertTrue(True)
        
    def test_DES_tarball_ch_to_stage_dir(self):
        self.assertTrue(True)
        
    def test_DES_tarball_get_filesize(self):
        self.assertTrue(True)
        
    def test_DES_tarball_execute_tar(self):
        self.assertTrue(True)
        
    def test_DES_tarball_get_md5sum(self):
        self.assertTrue(True)
        
    def test_DES_tarball_get_tar_name(self):
        self.assertTrue(True)
        

class TestDES_archive(unittest.TestCase):
    def test_DES_archive_init(self):
        self.assertTrue(True)
        
    def test_DES_archive_change_to_staging_dir(self):
        self.assertTrue(True)
        
    def test_DES_archive_make_directory_tar(self):
        self.assertTrue(True)
        
    def test_DES_archive_make_directory_tars(self):
        self.assertTrue(True)
        
    def test_DES_archive_generate(self):
        self.assertTrue(True)
        
    def test_DES_archive_update_db_tape(self):
        self.assertTrue(True)
        
    def test_DES_archive_update_db_unit(self):
        self.assertTrue(True)
        
    def test_DES_archive_return_key_value(self):
        self.assertTrue(True)
        
    def test_DES_archive_print_vars(self):
        self.assertTrue(True)
        
    def test_DES_archive_restore(self):
        self.assertTrue(True)
        

class TestArchiveSetup(unittest.TestCase):
    def test_main(self):
        self.assertTrue(True)
        
    def test_get_db(self):
        self.assertTrue(True)
        
    def test_get_sne(self):
        self.assertTrue(True)
        
    def test_get_raw(self):
        self.assertTrue(True)
        
    def test_get_all_raw(self):
        self.assertTrue(True)
        
    def test_add_dirs(self):
        self.assertTrue(True)
        
    def test_junk_runs(self):
        self.assertTrue(True)
        
    def test_parse_options(self):
        self.assertTrue(True)
        

class TestHungjobs(unittest.TestCase):
    def test_parse_cmdline(self):
        self.assertTrue(True)
        
    def test_query_attempts(self):
        self.assertTrue(True)
        
    def test_query_tasks(self):
        self.assertTrue(True)
        
    def test_connect(self):
        self.assertTrue(True)
        
    def test_make_tree(self):
        self.assertTrue(True)
        
    def test_write_tree(self):
        self.assertTrue(True)
        
    def test_find_hung(self):
        self.assertTrue(True)
        
    def test_find_trans_hung(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestJobmonitor(unittest.TestCase):
    def test_Task_init(self):
        self.assertTrue(True)
        
    def test_Task_add_child(self):
        self.assertTrue(True)
        
    def test_Task_str(self):
        self.assertTrue(True)
        
    def test_Task_is_running(self):
        self.assertTrue(True)
        
    def test_Task_len(self):
        self.assertTrue(True)
        
    def test_Task_last(self):
        self.assertTrue(True)
        
    def test_Print_init(self):
        self.assertTrue(True)
        
    def test_Print_write(self):
        self.assertTrue(True)
        
    def test_Print_close(self):
        self.assertTrue(True)
        
    def test_Print_flush(self):
        self.assertTrue(True)
        
    def test_Err_init(self):
        self.assertTrue(True)
        
    def test_Err_write(self):
        self.assertTrue(True)
        
    def test_Err_close(self):
        self.assertTrue(True)
        
    def test_Err_flush(self):
        self.assertTrue(True)
        
    def test_parse_cmdline(self):
        self.assertTrue(True)
        
    def test_query_attempts(self):
        self.assertTrue(True)
        
    def test_save_att_taskids(self):
        self.assertTrue(True)
        
    def test_query_tasks(self):
        self.assertTrue(True)
        
    def test_connect(self):
        self.assertTrue(True)
        
    def test_make_tree(self):
        self.assertTrue(True)
        
    def test_write_tree(self):
        self.assertTrue(True)
        
    def test_header(self):
        self.assertTrue(True)
        
    def test_find_hung(self):
        self.assertTrue(True)
        
    def test_h2(self):
        self.assertTrue(True)
        
    def test_find_trans_hung(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestMonitor(unittest.TestCase):
    def test_ProcMon_init(self):
        self.assertTrue(True)
        
    def test_parse_options(self):
        self.assertTrue(True)
        
    def test_getproc(self):
        self.assertTrue(True)
        
    def test_proc(self):
        self.assertTrue(True)
        
    def test_todatetime(self):
        self.assertTrue(True)
        
    def test_monitorp(self):
        self.assertTrue(True)
        
    def test_get_size(self):
        self.assertTrue(True)
        
    def test_get_size_db(self):
        self.assertTrue(True)
        
    def test_df(self):
        self.assertTrue(True)
        
    def test_get_tape_data(self):
        self.assertTrue(True)
        
    def test_get_backupdir_data(self):
        self.assertTrue(True)
        
    def test_get_deprecated(self):
        self.assertTrue(True)
        
    def test_get_database(self):
        self.assertTrue(True)
        
    def test_get_untransferred(self):
        self.assertTrue(True)
        
    def test_get_total_data(self):
        self.assertTrue(True)
        
    def test_report_processes(self):
        self.assertTrue(True)
        
    def test_report_untransferred(self):
        self.assertTrue(True)
        
    def test_report_archive_status(self):
        self.assertTrue(True)
        
    def test_historical(self):
        self.assertTrue(True)
        
    def test_get_db_holdings(self):
        self.assertTrue(True)
        
    def test_db_status(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestRestore_file(unittest.TestCase):
    def test_get_tape(self):
        self.assertTrue(True)
        
    def test_restore_files(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestRun_backup_db(unittest.TestCase):
    def test_parse_options(self):
        self.assertTrue(True)
        
    def test_archive_files(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestRun_backup(unittest.TestCase):
    def test_parse_options(self):
        self.assertTrue(True)
        
    def test_archive_files(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestTrack(unittest.TestCase):
    def test_parse_cmdline(self):
        self.assertTrue(True)
        
    def test_query_attempts(self):
        self.assertTrue(True)
        
    def test_save_att_taskids(self):
        self.assertTrue(True)
        
        self.assertTrue(True)
        
    def test_query_tasks(self):
        self.assertTrue(True)
        
    def test_connectmake_tree(self):
        self.assertTrue(True)
        
    def test_write_tree(self):
        self.assertTrue(True)
        
    def test_find_hung(self):
        self.assertTrue(True)
        
    def test_find_trans_hung(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestTransfer(unittest.TestCase):
    def test_parse_options(self):
        self.assertTrue(True)
        
    def test_Transfer_init(self):
        self.assertTrue(True)
        
    def test_Transfer_transfer(self):
        self.assertTrue(True)
        
    def test_Transfer_get_tries(self):
        self.assertTrue(True)
        
    def test_main(self):
        self.assertTrue(True)
        

class TestWhereis(unittest.TestCase):
    def test_parse_options(self):
        # test parsing the command line options
        temp = copy.deepcopy(sys.argv)
        svcs = 'my.ini'
        section = 'db_sec'
        filename = 'test.fits'
        sys.argv = ['where_is.py',
                    '--debug',
                    '--des_services=%s' % svcs,
                    '--section=%s' % section,
                    '--filename=%s' % filename
        ]
        args = wis.parse_options()
        self.assertTrue(args['debug'])
        self.assertEqual(args['des_services'], svcs)
        self.assertEqual(args['section'], section)
        self.assertEqual(args['filename'], filename)
        self.assertEqual(args['archive'], 'desar2home')
        sys.argv = temp
    
    @patch('where_is.Util')
    def test_main(self, mockUitl):
        # test for no archiving of the file yet
        temp = copy.deepcopy(sys.argv)
        svcs = 'my.ini'
        section = 'db_sec'
        filename = 'test.fits'
        sys.argv = ['where_is.py',
                    '--des_services=%s' % svcs,
                    '--section=%s' % section,
                    '--filename=%s' % filename
        ]
        unitdate = datetime.datetime(2018, 6, 15, 20, 0, 18)
        transdate = datetime.datetime(2018, 6, 16, 3, 15, 25)
        tapedate = datetime.datetime(2018, 6, 16, 2, 55, 0)
        unitfile = 'myUnit.tar'
        tapefile = 'myTape.tar'
        onTape = {'unit':None,
                  'unitdate': None,
                  'tape': None,
                  'tapedate': None,
                  'transdate': None,
                  'arch_root': 'the_root',
                  'path': 'big/long/data/path'}
        with patch('where_is.locate') as mockLocate:
            mockLocate.return_value = onTape
            with capture_output() as (out,err):
                wis.main()
                output = out.getvalue().strip()
                self.assertFalse(unitfile in output)
                self.assertFalse(tapefile in output)

        # test where file has been put in a unit file
        onTape['unit'] = unitfile
        onTape['unitdate'] = unitdate
        with patch('where_is.locate') as mockLocate:
            mockLocate.return_value = onTape
            with capture_output() as (out, err):
                wis.main()
                output = out.getvalue().strip()
                self.assertTrue(unitfile in output)
                self.assertTrue(unitdate.strftime("%Y-%m-%d") in output)
                self.assertTrue('has not been added' in output)
                self.assertFalse(tapefile in output)

        # test where file has been put in tape file, but not transferred
        onTape['tape'] = tapefile
        onTape['tapedate'] = tapedate
        with patch('where_is.locate') as mockLocate:
            mockLocate.return_value = onTape
            with capture_output() as (out, err):
                wis.main()
                output = out.getvalue().strip()
                self.assertTrue(unitfile in output)
                self.assertTrue(unitdate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapedate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapefile in output)
                self.assertTrue('has not been transferred' in output)

        # test where file has been transferred
        onTape['transdate'] = transdate
        with patch('where_is.locate') as mockLocate:
            mockLocate.return_value = onTape
            with capture_output() as (out, err):
                wis.main()
                output = out.getvalue().strip()
                self.assertTrue(unitfile in output)
                self.assertTrue(unitdate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapedate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapefile in output)
                self.assertTrue(transdate.strftime("%Y-%m-%d") in output)

        # test the exception
        with patch('where_is.locate', side_effect=ValueError()):
            with self.assertRaises(Exception):
                wis.main()

        # test debug output
        sys.argv.append('--debug')
        with patch('where_is.locate') as mockLocate:
            mockLocate.return_value = onTape
            with capture_output() as (out, err):
                wis.main()
                output = out.getvalue().strip()
                self.assertTrue(unitfile in output)
                self.assertTrue(unitdate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapedate.strftime("%Y-%m-%d") in output)
                self.assertTrue(tapefile in output)
                self.assertTrue(transdate.strftime("%Y-%m-%d") in output)
                self.assertTrue(section in output)

        sys.argv = temp
        
    
if __name__ == '__main__':
    unittest.main()
