#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import mock
from contextlib import contextmanager
from io import StringIO

from MockDBI import MockConnection

import filemgmt.utils as utils
import filemgmt.disk_utils_local as dul

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestUtils(unittest.TestCase):
    def test_get_config_vals(self):
        arch = {"home" : "desar2",
                "reqnum": 15,
                "pfwid": 224433
                }
        conf = {"exec": "testexec",
                "taskid": 88776
                }
        keylist = {"home": "rEq",
                   "exec": "",
                   "taskid": "REQ",
                   "date": ""
                   }
        with self.assertRaisesRegex(SystemExit, '1') as se:
            with capture_output() as (out, _):
                info = utils.get_config_vals(None, None, keylist)
                output = out.getvalue().strip()
                self.assertTrue('Could not find' in output)

        info = utils.get_config_vals(arch, conf, keylist)
        self.assertEqual(arch["home"], info["home"])
        self.assertEqual(conf["taskid"], info["taskid"])
        self.assertEqual(len(info), 3)

    def test_convert_permissions(self):
        self.assertEqual(utils.convert_permissions(777), 'rwxrwxrwx')
        self.assertEqual(utils.convert_permissions(4444), 'r-Sr--r--')
        self.assertEqual(utils.convert_permissions(4777), 'rwsrwxrwx')
        self.assertEqual(utils.convert_permissions(2424), 'r---wSr--')
        self.assertEqual(utils.convert_permissions('2777'), 'rwxrwsrwx')
        self.assertEqual(utils.convert_permissions('024'), '----w-r--')

    def test_ls_ld(self):
        res = utils.ls_ld('junk')
        self.assertTrue('FileMgmt' in res)
        self.assertTrue('/' in res)

        os.mkdir('test12345')
        self.assertTrue('test12345' in utils.ls_ld('test12345'))
        os.rmdir('test12345')
        with open('testf1234', 'w') as fh:
            fh.write('12345\n')
        self.assertTrue('testf1234' in utils.ls_ld('testf1234'))
        os.remove('testf1234')

    def test_find_ls(self):
        with capture_output() as (out, _):
            utils.find_ls('tests')
            output = out.getvalue().strip()
            self.assertTrue('tests/test_FileMgmt.py' in output)

    def test_get_mount_point(self):
        res = utils.get_mount_point(os.getcwd())
        self.assertTrue(res.startswith('/'))
        self.assertTrue(res.count('/') == 1)

    def test_get_mounted_device(self):
        self.assertIsNone(utils.get_mounted_device(os.getcwd()))
        self.assertIsNotNone(utils.get_mounted_device(utils.get_mount_point(os.getcwd())))

    def test_reduce(self):
        self.assertEqual(utils.reduce(1023), '1023b')
        self.assertEqual(utils.reduce(1024), '1k')
        self.assertEqual(utils.reduce(1900), '1k')
        self.assertEqual(utils.reduce(10 * 1024 * 1024), '10M')
        self.assertEqual(utils.reduce(5 * 1024 * 1024 * 1024 * 1024), '5T')

    def test_getfs_space(self):
        tot, free, used = utils.get_fs_space(os.getcwd())
        self.assertTrue(tot > 0)
        self.assertTrue(free > 0)
        self.assertTrue(used > 0)

    def test_df_h(self):
        with capture_output() as (out, _):
            utils.df_h(os.getcwd())
            output = out.getvalue().strip()
            self.assertTrue('Filesystem' in output)

class Testdisk_utils_local(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.mkdir('tester')
        cls.fname = ['testf1234.test',
                     'testf45678.test.gz',
                     'testexist.test']
        cls.md5 = ['d577273ff885c3f84dadb8578bb41399',
                   '0feb70518ce6f193acfbc6ce285ebc99']
        with open('tester/' + cls.fname[0], 'w') as fh:
            fh.write('12345\n')

        with open('tester/' + cls.fname[1], 'w') as fh:
            fh.write('aabbddhhee\n')

    @classmethod
    def tearDownClass(cls):
        for i in cls.fname:
            try:
                os.remove('tester/' + i)
            except:
                pass
        os.rmdir('tester')

    def test_get_md5sum_file(self):
        md5 = dul.get_md5sum_file(self.fname[0])
        self.assertEqual(md5, self.md5[0])

    def test_get_single_file_disk_info(self):
        res = dul.get_single_file_disk_info(self.fname[0])
        self.assertEqual(res['filename'], self.fname[0])
        self.assertIsNone(res['compression'])
        self.assertIsNone(res['path'])
        self.assertEqual(res['filesize'], 6)
        self.assertTrue('md5sum' not in res)

        res = dul.get_single_file_disk_info('./' + self.fname[0], True, 'rootpath')
        self.assertEqual(res['md5sum'], self.md5[0])
        self.assertIsNotNone(res['path'])
        self.assertTrue('relpath' not in res)

        res = dul.get_single_file_disk_info(os.getcwd() + '/' + self.fname[0], True, '/rootpath')
        self.assertIsNotNone(res['relpath'])

        os.environ['DISK_UTILS_LOCAL_DEBUG'] = '3'
        with capture_output() as (out, _):
            res = dul.get_single_file_disk_info(os.getcwd() + '/' + self.fname[1], True, '/rootpath')
            output = out.getvalue().strip()
            self.assertIsNotNone(res['compression'])
            self.assertEqual(res['filesize'], 11)
            self.assertTrue('fname' in output)
            self.assertTrue('path' in output)
        os.environ['DISK_UTILS_LOCAL_DEBUG'] = '0'

    def test_get_file_disk_info_list(self):
        res = dul.get_file_disk_info_list(self.fname)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[self.fname[0]]['filesize'], 6)
        self.assertTrue('err' in res[self.fname[2]])

    def test_get_file_disk_info(self):
        res = dul.get_file_disk_info(self.fname)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[self.fname[0]]['filesize'], 6)
        self.assertTrue('err' in res[self.fname[2]])


        res = dul.get_file_disk_info(os.getcwd() + '/tester')
        self.assertEqual(len(res), 2)

        with self.assertRaisesRegex(SystemExit, '1') as se:
            with capture_output() as (out, _):
                res = dul.get_file_disk_info(3)
                output = out.getvalue().strip()
                self.assertTrue('argument list' in output)

    def test_get_file_disk_info_path(self):
        res = dul.get_file_disk_info_path(os.getcwd() + '/tester')
        self.assertEqual(len(res), 2)
        fullname = os.getcwd() + '/tester/' + self.fname[0]
        self.assertEqual(res[fullname]['filename'], self.fname[0])
        self.assertIsNone(res[fullname]['compression'])
        self.assertIsNotNone(res[fullname]['path'])
        self.assertEqual(res[fullname]['filesize'], 6)

        with self.assertRaisesRegex(SystemExit, '1') as se:
            with capture_output() as (out, _):
                res = dul.get_file_disk_info_path('junkjunk')
                output = out.getvalue().strip()
                self.assertTrue('does not exist' in output)

if __name__ == '__main__':
    unittest.main()
