"""
Routines for performing tasks on files available through http.
"""

import os
import sys
import subprocess
import re
import traceback
import time
import certifi
import pycurl

import despyserviceaccess.serviceaccess as serviceaccess
import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.utils as utils


def http_code_str(hcode):
    codestr = f"Unmapped http_code ({hcode})"
    code2str = {'200': 'Success/Ok',
                '201': 'Success/Created',
                '204': 'No content (unknown status)',
                '301': 'Directory already existed',
                '304': 'Not modified',
                '400': 'Bad Request (check command syntax)',
                '401': 'Unauthorized (check username/password)',
                '403': 'Forbidden (check url, check perms)',
                '404': 'Not Found (check url exists and is readable)',
                '405': 'Method not allowed',
                '429': 'Too Many Requests (check transfer throttling)',
                '500': 'Internal Server Error',
                '501': 'Not implemented/understood',
                '507': 'Insufficient storage (check disk space)'}


    # convert given code to str (converting to int can fail)
    if str(hcode) in code2str:
        codestr = code2str[str(hcode)]
    return codestr

class HttpUtils:
    copyfiles_called = 0

    def __init__(self, des_services, des_http_section, numtries=5, secondsBetweenRetries=30):
        """Get password for curl and initialize existing_directories variable.

        >>> C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
        >>> len(C.curl_password)
        25"""
        try:
            # Parse the .desservices.ini file:
            self.auth_params = serviceaccess.parse(des_services, des_http_section)

            # Create the user/password switch:
            self.curl_password = f"{self.auth_params['user']}:{self.auth_params['passwd']}"
        except Exception as err:
            miscutils.fwdie(f"Unable to get curl password ({err})", fmdefs.FM_EXIT_FAILURE)
        self.curl = pycurl.Curl()
        self.curl.setopt(pycurl.USERPWD, self.curl_password)
        self.existing_directories = set()
        self.numtries = numtries
        self.src = None
        self.dst = None
        self.filesize = None
        self.secondsBetweenRetries = secondsBetweenRetries

    def reset(self):
        self.curl.reset()
        self.curl.setopt(pycurl.USERPWD, self.curl_password)

    def check_url(self, P):
        """See if P is a url.

        >>> C = HttpUtils('test_http_utils/.desservices.ini', 'file-http')
        >>> C.check_url("http://desar2.cosmology.illinois.edu")
        ('http://desar2.cosmology.illinois.edu', True)
        >>> C.check_url("hello")
        ('hello', False)"""
        if re.match("^https?:", P):
            if re.match(r"^https:", P):
                self.curl.setopt(pycurl.CAINFO, certifi.where())
            return (P, True)
        return (P, False)

    def verify(self):
        """ Method to verify if a file was completely transferred

        """
        try:
            self.curl.setopt(pycurl.URL, self.dst)
            self.curl.setopt(pycurl.NOBODY, 1)
            for i in range(self.numtries):
                self.curl.perform()
                rtemp = self.curl.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD)
                if rtemp is not None:
                    if i > 0:
                        print(f"Verify took {int(i) + 1} tries to succeed")
                    break
                time.sleep(5)
                if i == self.numtries - 1:
                    print(f"Failed to verify {self.dst} after {self.numtries:d} tries.")
                    return False
            rfsize = int(rtemp)
            if self.filesize == 0:
                self.filesize = os.path.getsize(self.src)
            if self.filesize == rfsize:
                return True
            return False
        except:
            (etype, value, trback) = sys.exc_info()
            traceback.print_exception(etype, value, trback, file=sys.stdout)
            return False
        finally:
            #self.curl.unsetopt(pycurl.URL)
            self.curl.setopt(pycurl.NOBODY, 0)

    def perform(self, cmd=None, verify=False, upload=False):
        for x in range(self.numtries):
            exitcode = pycurl.E_OK
            msg = None
            try:
                if upload:
                    self.curl.setopt(pycurl.UPLOAD, 1)
                self.curl.perform()
                if upload:
                    self.curl.setopt(pycurl.UPLOAD, 0)
            except pycurl.error as ex:
                exitcode, msg = ex.args
            httpcode = self.curl.getinfo(pycurl.HTTP_CODE)
            if exitcode == pycurl.E_OK:
                if ((httpcode in [200, 201, 301] and verify) or httpcode == 204) and self.dst is not None:
                    # if we get code 204 check to see if it has been transferred by getting file size
                    # check file size

                    if self.verify():
                        if x > 0:
                            print(f"Transfer took {x + 1} tries to succeed")
                        return
                elif httpcode in [200, 201, 301] and not verify:
                    if x > 0:
                        print(f"Transfer took {x + 1} tries to succeed")
                    return

            miscutils.fwdebug_print("*" * 75)
            miscutils.fwdebug_print("CURL FAILURE")
            miscutils.fwdebug_print(f"curl command: {cmd}")
            miscutils.fwdebug_print(f"curl exitcode: {exitcode} ({msg})")
            if httpcode is not None:
                miscutils.fwdebug_print(f"curl http status: {httpcode} ({http_code_str(httpcode)})")
            else:
                miscutils.fwdebug_print("curl http status: unknown")

            if x < self.numtries-1:    # not the last time in the loop
                miscutils.fwdebug_print(f"Sleeping {self.secondsBetweenRetries} secs")
                time.sleep(self.secondsBetweenRetries)
            else:
                print("\nDiagnostics:")
                print("Directory info")
                sys.stdout.flush()
                if miscutils.fwdebug_check(10, "HTTP_UTILS_DEBUG"):
                    utils.find_ls('.')
                elif self.src is not None:
                    print(utils.ls_ld(self.src))
                else:
                    print(os.getcwd())
                    print("Source file is not local")
                print("\nFile system disk space usage")
                utils.df_h('.')

                sys.stdout.flush()

                if self.src is not None:
                    hostm = re.search(r"https?://([^/:]+)[:/]", self.src)
                else:
                    hostm = re.search(r"https?://([^/:]+)[:/]", self.dst)
                if hostm:
                    hname = hostm.group(1)
                    try:   # don't let exception here halt
                        print(f"Running commands to {hname} for diagnostics")
                        print(f"\nPinging {hname}")
                        sys.stdout.flush()
                        stat = subprocess.Popen(f"ping -c 4 {hname}",
                                                shell=True,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                text=True)
                        curl_stdout = stat.communicate()[0]
                        print(curl_stdout)
                        print(f"\nRunning traceroute to {hname}")
                        sys.stdout.flush()
                        stat = subprocess.Popen(f"traceroute {hname}",
                                                shell=True,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                text=True)
                        curl_stdout = stat.communicate()[0]
                        print(curl_stdout)
                    except:   # print exception but continue
                        (etype, value, trback) = sys.exc_info()
                        traceback.print_exception(etype, value, trback, file=sys.stdout)
                        print("\n\nIgnoring remote diagnostics exception.   Continuing.\n")
                else:
                    print("Couldn't find url in curl cmd:", cmd)
                    print("Skipping remote diagnostics.\n")

                print("*" * 75)
                sys.stdout.flush()

        errmsg = f"Curl operation failed with return code {exitcode:d} ({msg}), "
        if httpcode is not None:
            errmsg += f" http status {httpcode} ({http_code_str(httpcode)})"
        else:
            errmsg += " http status unknown"

        raise Exception(errmsg)

    def create_http_intermediate_dirs(self, f):
        """Create all directories that are valid prefixes of the URL *f*.

        """
        # Making bar/ sometimes returns a 301 status even if there doesn't seem to be a bar/ in the directory.
        m = re.match(r"(https?://[^/]+)(/.*)", f)
        self.curl.setopt(pycurl.CUSTOMREQUEST, 'MKCOL')
        self.curl.setopt(pycurl.WRITEFUNCTION, lambda x: None)
        for x in miscutils.get_list_directories([m.group(2)]):
            if x not in self.existing_directories:
                self.curl.setopt(pycurl.URL, m.group(1) + x)
                if self.perform(cmd=f'MKCOL {m.group(1) + x}'):
                    self.existing_directories.add(x)
        self.curl.unsetopt(pycurl.CUSTOMREQUEST)
        #self.curl.unsetopt(pycurl.URL)


    def get(self, verify=False):
        starttime = time.time()
        self.curl.setopt(pycurl.URL, self.src)
        self.curl.setopt(pycurl.WRITEFUNCTION, open(self.dst, 'wb').write)
        self.perform(cmd=f'Get {self.src}', verify=verify)
        #self.curl.unsetopt(pycurl.WRITEFUNCTION)
        #self.curl.unsetopt(pycurl.URL)
        return time.time() - starttime

    def put(self, verify=False):
        starttime = time.time()
        self.curl.setopt(pycurl.URL, self.dst)
        #self.curl.setopt(pycurl.UPLOAD, 1)
        self.curl.setopt(pycurl.READFUNCTION, open(self.src, 'rb').read)
        # suppress screen output
        self.curl.setopt(pycurl.WRITEFUNCTION, lambda x: None)
        if self.filesize == 0:
            self.filesize = os.path.getsize(self.src)
        self.curl.setopt(pycurl.INFILESIZE, self.filesize)
        self.perform(cmd=f'PUT {self.src} to {self.dst}', verify=verify, upload=True)
        #self.curl.unsetopt(pycurl.WRITEFUNCTION)
        #self.curl.unsetopt(pycurl.URL)
        #self.curl.unsetopt(pycurl.INFILESIZE)
        #self.curl.setopt(pycurl.UPLOAD, 0)
        #self.curl.unsetopt(pycurl.READFUNCTION)
        return time.time() - starttime


    def copyfiles(self, filelist, tstats, secondsBetweenRetriesC=30, numTriesC=5, verify=False):
        """ Copies files in given src,dst in filelist """
        num_copies_from_archive = 0
        num_copies_to_archive = 0
        total_copy_time_from_archive = 0.0
        total_copy_time_to_archive = 0.0
        status = 0
        self.secondsBetweenRetries = secondsBetweenRetriesC
        self.numtries = numTriesC
        try:
            for filename, fdict in filelist.items():
                self.filesize = 0
                if 'filesize' in fdict and fdict['filesize'] is not None:
                    self.filesize = fdict['filesize']
                try:
                    (self.src, isurl_src) = self.check_url(fdict['src'])
                    (self.dst, isurl_dst) = self.check_url(fdict['dst'])
                    if (isurl_src and isurl_dst) or (not isurl_src and not isurl_dst):
                        miscutils.fwdie(f"Exactly one of isurl_src and isurl_dst has to be true (values: {isurl_src}, {self.src}, {isurl_dst}, {self.dst}",
                                        fmdefs.FM_EXIT_FAILURE)

                    copy_time = None

                    # if local file and file doesn't already exist
                    if not isurl_dst and not os.path.exists(self.dst):
                        if tstats is not None:
                            tstats.stat_beg_file(filename)

                        # make the path
                        path = os.path.dirname(self.dst)
                        if path and not os.path.exists(path):
                            miscutils.coremakedirs(path)

                        # getting some non-zero curl exit codes, double check path exists
                        if path and not os.path.exists(path):
                            raise Exception(f"Error: path still missing after coremakedirs ({path})")
                        copy_time = self.get(verify)
                        if tstats is not None:
                            tstats.stat_end_file(0, self.filesize)
                    elif isurl_dst:   # if remote file
                        if tstats is not None:
                            tstats.stat_beg_file(filename)

                        # create remote paths
                        self.create_http_intermediate_dirs(self.dst)
                        copy_time = self.put(verify)

                        if tstats is not None:
                            tstats.stat_end_file(0, self.filesize)

                    # Print some debugging info:
                    if miscutils.fwdebug_check(9, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print("\n")
                        for lines in traceback.format_stack():
                            for L in lines.split('\n'):
                                if L.strip() != '':
                                    miscutils.fwdebug_print(f"call stack: {L}")

                    if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print(f"Copy info: {HttpUtils.copyfiles_called} {fdict['filename']} {self.filesize} {copy_time} {time.time()} {'toarchive' if isurl_dst else 'fromarchive'}")

                    if copy_time is None:
                        copy_time = 0

                    if isurl_dst:
                        num_copies_to_archive += 1
                        total_copy_time_to_archive += copy_time
                    else:
                        num_copies_from_archive += 1
                        total_copy_time_from_archive += copy_time

                except Exception as err:
                    status = 1
                    if tstats is not None:
                        tstats.stat_end_file(1, self.filesize)
                    filelist[filename]['err'] = str(err)
                    miscutils.fwdebug_print(str(err))

        finally:
            print(f"[Copy summary] copy_batch:{HttpUtils.copyfiles_called:d}  file_copies_to_archive:{num_copies_to_archive:d} time_to_archive:{total_copy_time_to_archive:.3f} copies_from_archive:{num_copies_from_archive:d} time_from_archive:{total_copy_time_from_archive:.3f}  end_time_for_batch:{time.time():3f}")

        HttpUtils.copyfiles_called += 1
        return (status, filelist)
