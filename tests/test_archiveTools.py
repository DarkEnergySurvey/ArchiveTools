#!/usr/bin/env python2
import matplotlib
matplotlib.use('PS')
import unittest
from contextlib import contextmanager
from StringIO import StringIO
from mock import patch, mock_open
from archivetools import backup_util as bu
from archivetools import DES_tarball as dt
from archivetools import DES_archive as da
import sys
import copy
import datetime
sys.path.append('bin')
sys.path.append('tests')

import os

import where_is as wis

MD5TESTSUM = '23d899a47f09b776213ae'

class MockDbi(object):
    def __init__(self, *args, **kwargs):
        self.data = [(('the_root',),)]
        self.con = self.Connection()

    def cursor(self):
        return self.Cursor(self.data)

    def setThrow(self, value):
        self.con.throw = value

    class Connection(object):
        def __init__(self):
            self.throw = False

        def ping(self):
            if self.throw:
                raise Exception()
            return True

    class Cursor(object):
        def __init__(self, data = []):
            self.data = data
            self.data.reverse()

        def setData(self, data):
            self.data = data
            self.data.reverse()

        def execute(self,*args, **kwargs):
            pass

        def fetchall(self):
            if self.data:
                return self.data.pop()
            return None
    def setReturn(self, data):
        self.data = data

class MockUtil(object):
    def __init__(self):
        self.data = []
        self.ckeckVals = [True,False]
            
    class Cursor(object):
        def __init__(self, data = []):
            self.data = data
            self.data.reverse()
                
        def setData(self, data):
            self.data = data
            self.data.reverse()

        def execute(self,*args, **kwargs):
            pass

        def fetchall(self):
            if self.data:
                return self.data.pop()
            return None
        
        def prepare(self, *args, **kwargs):
            pass
            
    def setReturn(self, data):
        self.data = data
            
    def cursor(self):
        return self.Cursor(self.data)
    
    def generate_md5sum(self, *args, **kwargs):
        return MD5TESTSUM
    
    def checkFiles(self, *args, **kwargs):
        return self.checkVals.pop()
    
    def log(self, *args, **kwargs):
        return

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
        archive_root = '/archive/root'
        unit_name = 'testUnit'
        archive_root_rtn = ((archive_root,),)
        archive_path = 'the/path/you/want'
        file_path = 'file/path'
        fileNoPath = ((archive_path,),)
        filePath = ((file_path, archive_path,),)
        created_date = datetime.datetime(2018, 3, 15, 15, 47,20)
        tape_tar = 'theTape.tar'
        tape_date = datetime.datetime(2018, 3, 16, 0, 22, 8)
        transfer_date = datetime.datetime(2018, 3, 16, 7, 8, 2)
        rootpath = 'my/root/path'

        myMock = MockUtil()

        # check for exception if not enough arguments are given
        with self.assertRaises(ValueError):
            bu.locate(myMock)

        # check for exception if not enough arguments are given
        with self.assertRaises(ValueError):
            bu.locate(myMock, reqnum=1234)

        # check for exception if not enough arguments are given
        with self.assertRaises(ValueError):
            bu.locate(myMock, reqnum=1234, unitname='uunm')

        # check for exception if not enough arguments are given
        with self.assertRaises(ValueError):
            bu.locate(myMock, unitname='uunm',attnum='2')

        # get just file info
        myMock.setReturn([archive_root_rtn,
                          filePath])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertIsNone(results['unit'])

        # get just file info
        myMock.setReturn([None,
                          filePath])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertIsNone(results['arch_root'])
        self.assertEqual(results['path'], archive_path)
        self.assertIsNone(results['unit'])

        # get just file info with pfwid
        myMock.setReturn([archive_root_rtn,
                          fileNoPath])
        results = bu.locate(myMock, pfwid=12345, archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertIsNone(results['unit'])

        # get just file info with pfwid, pfwid not existing
        myMock.setReturn([archive_root_rtn,
                          None])
        results = bu.locate(myMock, pfwid=12345, archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertIsNone(results['path'])
        self.assertIsNone(results['unit'])

        # get just file info with triplet
        myMock.setReturn([archive_root_rtn,
                          fileNoPath])
        results = bu.locate(myMock, reqnum=12345, unitname='uun', attnum='2', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertIsNone(results['unit'])

        # get just file info with pfwid, pfwid not existing
        myMock.setReturn([archive_root_rtn,
                          None])
        results = bu.locate(myMock,  reqnum=12345, unitname='uun', attnum='2', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertIsNone(results['path'])
        self.assertIsNone(results['unit'])

        # no unit file
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          ((None,),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertIsNone(results['unit'])

        # have unit file on second iteration
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          None,
                          ((unit_name,),),
                          ((created_date, None),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)

        # have unit file
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          ((unit_name,),),
                          ((created_date, None),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertIsNone(results['tape'])

        # have unit file, with compression in filename
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          ((unit_name,),),
                          ((created_date, None),)])
        results = bu.locate(myMock, filename='myfile.fits.fz', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertIsNone(results['tape'])

        # have unit file
        myMock.setReturn([archive_root_rtn,
                          None,
                          fileNoPath,
                          ((unit_name,),),
                          ((created_date, None),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertIsNone(results['tape'])

        # have unit file, with compression in filename
        myMock.setReturn([archive_root_rtn,
                          None,
                          fileNoPath,
                          ((unit_name,),),
                          ((created_date, None),)])
        results = bu.locate(myMock, filename='myfile.fits.fz', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertIsNone(results['tape'])

        # file does not exist
        myMock.setReturn([archive_root_rtn,
                          None,
                          None])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertIsNone(results['path'])
        self.assertIsNone(results['unit'])

        # have unit and tape tar, but no date
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          ((unit_name,),),
                          ((created_date, tape_tar),),
                          ((None, None),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertEqual(results['tape'], tape_tar)
        self.assertIsNone(results['tapedate'])

        # have tape tar and dates
        myMock.setReturn([archive_root_rtn,
                          filePath,
                          ((unit_name,),),
                          ((created_date, tape_tar),),
                          ((tape_date, transfer_date),)])
        results = bu.locate(myMock, filename='myfile', archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], archive_path)
        self.assertEqual(results['unit'], unit_name)
        self.assertEqual(results['unitdate'], created_date)
        self.assertEqual(results['tape'], tape_tar)
        self.assertEqual(results['tapedate'], tape_date)
        self.assertEqual(results['transdate'], transfer_date)

        # get just file info with root path
        myMock.setReturn([archive_root_rtn])
        results = bu.locate(myMock, rootpath=rootpath, archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], rootpath)
        self.assertIsNone(results['unit'])

        # get just file info with root path
        myMock.setReturn([archive_root_rtn])
        results = bu.locate(myMock, rootpath='/'+rootpath, archive='myarch')
        self.assertEqual(results['arch_root'], archive_root)
        self.assertEqual(results['path'], '/'+rootpath)
        self.assertIsNone(results['unit'])

        # get just file info with root path, no archive
        myMock.setReturn([archive_root_rtn])
        results = bu.locate(myMock, rootpath='/'+rootpath)
        self.assertIsNone(results['arch_root'])
        self.assertIsNone(results['path'])
        self.assertIsNone(results['unit'])

        # get just file info with root path, no archive
        myMock.setReturn([None])
        results = bu.locate(myMock, rootpath='/'+rootpath, archive='myarch')
        self.assertIsNone(results['arch_root'])
        self.assertIsNone(results['path'])
        self.assertIsNone(results['unit'])


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

    @patch('archivetools.backup_util.Util')
    @patch('archivetools.backup_util.os')
    @patch('archivetools.backup_util.shutil')
    def test_check_files(self, utilPatch, ospatch, shpatch):
        md5s = ['005ad754bf7840202c382989472ed560',
                '56b6f70fe8f57c4e912caea7fe43de20',
                'bb53b347b33632d2e8f6c8389ae003a7']
        badmd5 = ['005ad754bf7840202c382989472ed560',
                  '56b6f70fe8f57c4e962caea7fe43de20',
                  'bb53b347b33632d2e8f6c8389ae003a7']
        filenames = ['testfile.dat',
                     'anotherfile.dat',
                     'lasfile.d']
        data = {filenames[0]: [None, md5s[0]],
                filenames[1]: [None, md5s[1]],
                filenames[2]: [None, md5s[2]]}
        walkvals = [['d1', '', [filenames[0]]],
                    ['d3', '', [filenames[1]]],
                    ['dd', '', [filenames[2]]]]
        with patch('archivetools.backup_util.tarfile.open') as tarpatch:
            with patch('archivetools.backup_util.os.walk', return_value=walkvals) as m:
                with patch('archivetools.backup_util.generate_md5sum', side_effect=md5s) as md:
                    self.assertTrue(bu.check_files(data,'tt', 'ac.tar', utilPatch))
        
        # now test a failure
        with patch('archivetools.backup_util.tarfile.open') as tarpatch:
            with patch('archivetools.backup_util.os.walk', return_value=walkvals) as m:
                with patch('archivetools.backup_util.generate_md5sum', side_effect=badmd5) as md:
                    self.assertFalse(bu.check_files(data,'tt', 'ac.tar', utilPatch))
        
    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    def test_Util_init(self):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None)
        self.assertEqual(util.root, 'the_root')

    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    def test_Util_ping(self):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None)
        self.assertTrue(util.ping())

        util.setThrow(True)
        self.assertFalse(util.ping())
        
    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    def test_Util_reconnect(self):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None)
        util.reconnect()

    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    def test_Util_init_logger(self):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None)
        
    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    @patch('archivetools.backup_util.MIMEText')
    @patch('archivetools.backup_util.smtplib')
    def test_Util_notify(self, mimeMock, smtpMock):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None)
        with self.assertRaises(Exception):
            util.notify(1,'my msg')
        util = bu.Util(None, None, 'my.log', 'mytype')
        util.notify(1, 'my msg')

        util.notify(100, 'bad msg')
        util.notify(35, 'err msg')
        util.notify(25, 'warn msg')
        util.notify(1, 'log msg', 'test@email.com')

    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    @patch('archivetools.backup_util.MIMEText')
    @patch('archivetools.backup_util.smtplib')
    def test_Util_log(self, mimeMock, smtpMock):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None, 'my.log', 'mytype')
        util.log(10,'my msg')
        
    @patch('archivetools.backup_util.desdmdbi.DesDmDbi', MockDbi)
    @patch('archivetools.backup_util.MIMEText')
    @patch('archivetools.backup_util.smtplib')
    def test_Util_checkfreespace(self, mimeMock, smtpMock):
        bu.Util.__bases__ = (MockDbi,)
        util = bu.Util(None, None, 'my.log', 'mytype')
        self.assertTrue(util.checkfreespace('.'))
        util.reqfree = 118442389504*1000
        self.assertFalse(util.checkfreespace('.'))

    @patch('archivetools.backup_util.pyplot')
    def test_Plot_init(self, plotMock):
        pl = bu.Plot('myfile')
        pl2 = bu.Plot('myfile2', 'TitleS')
        
    @patch('archivetools.backup_util.pyplot')
    def test_Plot_save(self, plotMock):
        pl = bu.Plot('myfile')
        pl.save()

    @patch('archivetools.backup_util.pyplot')
    def test_Plot_generate(self, plotMock):
        pl = bu.Plot('myfile')
        with capture_output() as (out, err):
            pl.generate()
            output = out.getvalue().strip()
            self.assertTrue('Not implemented' in output)

    @patch('archivetools.backup_util.pyplot')
    def test_Pie_init(self, plotMock):
        pl = bu.Pie('myfile',[1,2],['l1','l2'])
        pl = bu.Pie('myFile2',[1,2],['l1','l2'], colors=['red','yellow'])
        with self.assertRaises(Exception):
            bu.Pie('myFile',[1,2,3],['l1','l2'])

    @patch('archivetools.backup_util.pyplot')
    def test_Pie_generate(self, plotMock):
        pl = bu.Pie('myfile',[1,2],['l1','l2'])
        pl.generate()
        
    @patch('archivetools.backup_util.pyplot')
    def test_BoxPlot_init(self, plotMock):
        bx = bu.BoxPlot('fname',[1,2,3])
        bx = bu.BoxPlot('fname',[1,2,3], xlabel='xlab',ylabel='ylab',colors=['red','green'])
        
    @patch('archivetools.backup_util.pyplot')
    def test_BoxPlot_add_ydata(self, plotMock):
        bx = bu.BoxPlot('fname',[1,2,3], xlabel='xlab',ylabel='ylab',colors=['red','green'])
        bx.add_ydata([4,5])
        bx.add_ydata([6,7], legend="my label")
        
    @patch('archivetools.backup_util.pyplot')
    def test_BoxPlot_generate(self, plotMock):
        bx = bu.BoxPlot('fname',[1,2,3], xlabel='xlab',ylabel='ylab',colors=['red','green'])
        bx.add_ydata([4,5])
        bx.generate()

        bx = bu.BoxPlot('fname',[1,2,3], xlabel='xlab',ylabel='ylab',xdate=True, dodots=True)
        bx.add_ydata([4,5],legend='lgnd1')
        bx.generate()
        

class TestDES_tarball(unittest.TestCase):

    @patch('archivetools.DES_tarball.tarfile')
    @patch('archivetools.DES_tarball.os')
    def test_DES_tarball_init(self, tarMock, osMock):
        theSize = 123456789
        tarFile = 'myTest.tar'
        thePath = '/the/tar/path'
        theArgs = {'stgdir': '.'}
        theItems = ['file.1', 'file.2']
        myMock = MockUtil()
        checks = [False, True]
        # basic init
        with patch('archivetools.DES_tarball.os.path.getsize', return_value=theSize) as g:
            with patch('archivetools.DES_tarball.os.getcwd', return_value='.') as gw:
                test = dt.DES_tarball({}, [], tarFile, myMock, thePath, theSize, MD5TESTSUM)
                self.assertEqual(test.get_tar_name(), tarFile)
                self.assertEqual(test.get_filesize(), theSize)
                self.assertEqual(test.get_md5sum(), MD5TESTSUM)
        
        # init with generating tarfile
        with patch('archivetools.DES_tarball.os.path.getsize', return_value=theSize) as g:
            with patch('archivetools.DES_tarball.os.getcwd', return_value='.') as gw:
                with patch('archivetools.DES_tarball.bu.generate_md5sum', return_value=MD5TESTSUM) as gm:
                    test = dt.DES_tarball(theArgs, theItems, {}, myMock, thePath, file_class=bu.CLASSES[0])
                    self.assertEqual(test.get_md5sum(), MD5TESTSUM)
                    self.assertNotEqual(test.get_tar_name(), tarFile)

        # verify file size
        # init with generating tarfile
        with patch('archivetools.DES_tarball.os.path.getsize', return_value=theSize) as g:
            with patch('archivetools.DES_tarball.os.getcwd', return_value='.') as gw:
                with patch('archivetools.DES_tarball.bu.generate_md5sum', return_value=MD5TESTSUM) as gm:
                    with patch('archivetools.DES_tarball.bu.check_files', side_effect=checks) as cf:
                        test = dt.DES_tarball(theArgs, theItems, {}, myMock, thePath, file_class=bu.CLASSES[0], verify=True, tarname=tarFile)
                        self.assertEqual(test.get_md5sum(), MD5TESTSUM)
                        self.assertEqual(test.get_tar_name(), tarFile)

class TestDES_archive(unittest.TestCase):

    @patch('archivetools.DES_archive.os')
    @patch('archivetools.DES_archive.DES_tarball')
    def test_DES_archive_init(self, osMock, tarMock):
        myMock = MockUtil()
        theArgs = {'stgdir': '.',
                   'xferdir': '.'}

        myMock.setReturn([(('tar1.tar',12345,MD5TESTSUM),
                          ('tar1.tar', 456789, MD5TESTSUM + 'a'))])
        with patch('archivetools.DES_archive.DES_tarball.tar_size', return_value=101010) as ts:
            test = da.DES_archive(theArgs, myMock, bu.CLASSES[3], 2)

        
    #def test_DES_archive_change_to_staging_dir(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_make_directory_tar(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_make_directory_tars(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_generate(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_update_db_tape(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_update_db_unit(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_return_key_value(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_print_vars(self):
    #    self.assertTrue(True)
        
    #def test_DES_archive_restore(self):
    #    self.assertTrue(True)
        

#class TestArchiveSetup(unittest.TestCase):
    #def test_main(self):
    #    self.assertTrue(True)
        
    #def test_get_db(self):
    #    self.assertTrue(True)
        
    #def test_get_sne(self):
    #    self.assertTrue(True)
        
    #def test_get_raw(self):
    #    self.assertTrue(True)
        
    #def test_get_all_raw(self):
    #    self.assertTrue(True)
        
    #def test_add_dirs(self):
    #    self.assertTrue(True)
        
    #def test_junk_runs(self):
    #    self.assertTrue(True)
        
    #def test_parse_options(self):
    #    self.assertTrue(True)
        

#class TestHungjobs(unittest.TestCase):
    #def test_parse_cmdline(self):
    #    self.assertTrue(True)
        
    #def test_query_attempts(self):
    #    self.assertTrue(True)
        
    #def test_query_tasks(self):
    #    self.assertTrue(True)
        
    #def test_connect(self):
    #    self.assertTrue(True)
        
    #def test_make_tree(self):
    #    self.assertTrue(True)
        
    #def test_write_tree(self):
    #    self.assertTrue(True)
        
    #def test_find_hung(self):
    #    self.assertTrue(True)
        
    #def test_find_trans_hung(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestJobmonitor(unittest.TestCase):
    #def test_Task_init(self):
    #    self.assertTrue(True)
        
    #def test_Task_add_child(self):
    #    self.assertTrue(True)
        
    #def test_Task_str(self):
    #    self.assertTrue(True)
        
    #def test_Task_is_running(self):
    #    self.assertTrue(True)
        
    #def test_Task_len(self):
    #    self.assertTrue(True)
        
    #def test_Task_last(self):
    #    self.assertTrue(True)
        
    #def test_Print_init(self):
    #    self.assertTrue(True)
        
    #def test_Print_write(self):
    #    self.assertTrue(True)
        
    #def test_Print_close(self):
    #    self.assertTrue(True)
        
    #def test_Print_flush(self):
    #    self.assertTrue(True)
        
    #def test_Err_init(self):
    #    self.assertTrue(True)
        
    #def test_Err_write(self):
    #    self.assertTrue(True)
        
    #def test_Err_close(self):
    #    self.assertTrue(True)
        
    #def test_Err_flush(self):
    #    self.assertTrue(True)
        
    #def test_parse_cmdline(self):
    #    self.assertTrue(True)
        
    #def test_query_attempts(self):
    #    self.assertTrue(True)
        
    #def test_save_att_taskids(self):
    #    self.assertTrue(True)
        
    #def test_query_tasks(self):
    #    self.assertTrue(True)
        
    #def test_connect(self):
    #    self.assertTrue(True)
        
    #def test_make_tree(self):
    #    self.assertTrue(True)
        
    #def test_write_tree(self):
    #    self.assertTrue(True)
        
    #def test_header(self):
    #    self.assertTrue(True)
        
    #def test_find_hung(self):
    #    self.assertTrue(True)
        
    #def test_h2(self):
    #    self.assertTrue(True)
        
    #def test_find_trans_hung(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestMonitor(unittest.TestCase):
    #def test_ProcMon_init(self):
    #    self.assertTrue(True)
        
    #def test_parse_options(self):
    #    self.assertTrue(True)
        
    #def test_getproc(self):
    #    self.assertTrue(True)
        
    #def test_proc(self):
    #    self.assertTrue(True)
        
    #def test_todatetime(self):
    #    self.assertTrue(True)
        
    #def test_monitorp(self):
    #    self.assertTrue(True)
        
    #def test_get_size(self):
    #    self.assertTrue(True)
        
    #def test_get_size_db(self):
    #    self.assertTrue(True)
        
    #def test_df(self):
    #    self.assertTrue(True)
        
    #def test_get_tape_data(self):
    #    self.assertTrue(True)
        
    #def test_get_backupdir_data(self):
    #    self.assertTrue(True)
        
    #def test_get_deprecated(self):
    #    self.assertTrue(True)
        
    #def test_get_database(self):
    #    self.assertTrue(True)
        
    #def test_get_untransferred(self):
    #    self.assertTrue(True)
        
    #def test_get_total_data(self):
    #    self.assertTrue(True)
        
    #def test_report_processes(self):
    #    self.assertTrue(True)
        
    #def test_report_untransferred(self):
    #    self.assertTrue(True)
        
    #def test_report_archive_status(self):
    #    self.assertTrue(True)
        
    #def test_historical(self):
    #    self.assertTrue(True)
        
    #def test_get_db_holdings(self):
    #    self.assertTrue(True)
        
    #def test_db_status(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestRestore_file(unittest.TestCase):
    #def test_get_tape(self):
    #    self.assertTrue(True)
        
    #def test_restore_files(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestRun_backup_db(unittest.TestCase):
    #def test_parse_options(self):
    #    self.assertTrue(True)
        
    #def test_archive_files(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestRun_backup(unittest.TestCase):
    #def test_parse_options(self):
    #    self.assertTrue(True)
        
    #def test_archive_files(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestTrack(unittest.TestCase):
    #def test_parse_cmdline(self):
    #    self.assertTrue(True)
        
    #def test_query_attempts(self):
    #    self.assertTrue(True)
        
    #def test_save_att_taskids(self):
    #    self.assertTrue(True)
        
    #def test_query_tasks(self):
    #    self.assertTrue(True)
        
    #def test_connectmake_tree(self):
    #    self.assertTrue(True)
        
    #def test_write_tree(self):
    #    self.assertTrue(True)
        
    #def test_find_hung(self):
    #    self.assertTrue(True)
        
    #def test_find_trans_hung(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

#class TestTransfer(unittest.TestCase):
    #def test_parse_options(self):
    #    self.assertTrue(True)
        
    #def test_Transfer_init(self):
    #    self.assertTrue(True)
        
    #def test_Transfer_transfer(self):
    #    self.assertTrue(True)
        
    #def test_Transfer_get_tries(self):
    #    self.assertTrue(True)
        
    #def test_main(self):
    #    self.assertTrue(True)
        

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
