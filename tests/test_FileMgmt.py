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
if __name__ == '__main__':
    unittest.main()
