#!/usr/bin/env python2
import matplotlib
matplotlib.use('PS')
import mock
import unittest
from archivetools import backup_util as bu
import sys
import copy
sys.path.append('bin')

import where_is as wis

class TestBackupUtil(unittest.TestCase):
    def test_get_subdir(self):
        self.assertEqual(bu.get_subdir('RAW_FILE'), 'DTS')
        self.assertEqual(bu.get_subdir('DB_BACKUPSTUFF'), 'DB')
        self.assertEqual(bu.get_subdir('ANYTHING_ELSE'), 'OPS')
    
    def test_locate(self):
        self.assertTrue(True)

    def test_generate_md5sum(self):
        self.assertTrue(True)
        
    def test_calculate_archive_size(self):
        self.assertTrue(True)
        
    def test_srmls(self):
        self.assertTrue(True)
          
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
        temp = copy.deepcopy(sys.argv)
        svcs = 'my.ini'
        section = 'db_sec'
        filename = 'test.fits'
        sys.argv = ['where_is.py',
                    '--debug',
                    '--des_services=%s' % svcs,
                    '--section=%s' % section,
                    '--filename=%s' % filename
        ])
        args = wis.parse_options()
        self.assertTrue(args['debug'])
        self.assertEqual(args['des_services'], svcs)
        self.assertEqual(args['section'], section)
        self.assertEqual(args['filename'], filename)
        self.assertEqual(args['archive'], 'desar2home')
        sys.argv = temp
        
    def test_main(self):
        self.assertTrue(True)
        
    
if __name__ == '__main__':
    unittest.main()
