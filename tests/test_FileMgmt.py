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



if __name__ == '__main__':
    unittest.main()
