# $Id: easywebdav_utils.py 42792 2016-06-22 19:25:07Z mgower $
# $Rev:: 42792                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-06-22 14:25:07 #$:  # Date of last commit.

"""
Routines for performing tasks on files available through http.
"""

__version__ = "$Rev: 42792 $"

import os
import sys
import shutil
import subprocess
import re
import traceback
import time

import despyserviceaccess.serviceaccess as serviceaccess
import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs
import deswebdav as webdav
import filemgmt.disk_utils_local as dul

def http_code_str(hcode):
    codestr = "Unmapped http_code (%s)" % hcode
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

class EwdUtils():
    copyfiles_called = 0

    def __init__(self, des_services, des_http_section, destination):
        """Class to handle data transfers. Uses easywebdav for the connection and transfers.

           Parameters
           ---------
           des_services : str
               Name of the services file

           des_http_section : str
               Name of the section in the services file which specified the connection information

           destination : str
               Name or IP of the destination host.

           >>> C = EwdUtils('test_http_utils/.desservices.ini', 'file-http', 'des.cosmology.edu')
        """
        try:
            # Parse the .desservices.ini file:
            self.auth_params = serviceaccess.parse(des_services, des_http_section)
            # set up the connection
            self.webdav = webdav.connect(destination, username=self.auth_params['user'], password=self.auth_params['passwd'])
        except Exception as err:
            miscutils.fwdie("Unable to get curl password (%s)" % err, fmdefs.FM_EXIT_FAILURE)

        self.existing_directories = set()

    def check_url(self, P):
        """ See if P is a url.
            
            Parameters
            ----------
            P : str
                The string to test if it is a url, based on it starting with http: or https:

            Returns
            -------
            Tuple of the input string and a boolean (True if it is a url, False otherwise)

            >>> C = EwdUtils('test_http_utils/.desservices.ini', 'file-http', 'des.cosmology.edu')
            >>> C.check_url("http://desar2.cosmology.illinois.edu")
            ('http://desar2.cosmology.illinois.edu', True)
            >>> C.check_url("hello")
            ('hello', False)
            """
        if re.match("^https?:", P):
            return (P, True)
        return (P, False)

    def move_file(self, src, dst, secondsBetweenRetries=30, numTries=5, isTest=False, verify=None, fdict={}):
        """ Method to move a single file, either upload or download (determined by which has a web address in
            the file name)

            Parameters
            ----------
            src : str
                The name of the source file

            dst : str
                The name of the destination file

            secondsBetweenRetries : int
                The number of seconds between transfer retries.
                Default: 30

            numTries : int
                The maximum number of times to try the transfer.
                Default: 5

            isTest : bool
                If True then the code is just being tested. False means normal transfer.
                Default: False

            verify : str
                Whether or not to verify the transfer, either checking md5sum or file size.
                Possible inputs are: None (no checking), "filesize" (check the file size),
                and "md5sum" (check the md5sum, only works on downloads).
                Default: None

            fdict : dict
                Dictionary of file attributes, used to check the md5sum.
                Default: {}

        """
        # track the transfer time
        starttime = time.time()

        # check to see which way the file is going (upload or download)
        (temp, upl) = self.check_url(dst)
        (temp, dnl) = self.check_url(src)

        # if downloading
        if dnl:
            loc = src.find("/",10)
            sfile = src[loc:]
            dfile = dst
            mode = "download"
            if verify is None:
                checksize = False
                checkmd5 = False
            elif verify.upper().startswith('F'):
                checksize = True
                checkmd5 = False
            elif verify.upper().startswith('M'):
                checksize = False
                checkmd5 = True
            else:
                print "Unknown verify command given, no verification will be done."
            down = True
        # if uploading
        elif upl:
            loc = dst.find("/",10)
            dfile = dst[loc:]
            sfile = src
            mode = "upload"
            down = False
            if verify is None:
                checksize = False
                checkmd5 = False
            elif verify.upper().startswith('F'):
                checksize = True
                checkmd5 = False
            elif verify.upper().startswith('M'):
                checksize = False
                checkmd5 = False
                print "Cannot verify md5 sum of uploaded files."
            else:
                print "Unknown verify command given, no verification will be done."
        # try to do the transfer
        for x in range(0, numTries):
            if not isTest:
                if x == 0:
                    if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                        miscutils.fwdebug_print("webdav %sing: %s" % (mode, sfile))
                else:
                    miscutils.fwdebug_print("Repeating %s command after failure (%d): %s" % (mode, x, sfile))
            if down:
                self.webdav.download(sfile, dfile)
            else:
                self.webdav.upload(sfile, dfile)
            # get the http status code
            httpcode = self.webdav.getstatus()
            httpstr = self.webdav.getreason()

            if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                miscutils.fwdebug_print("webdav %s: %s" % (mode, sfile))
                miscutils.fwdebug_print("http status: %i (%s)" % (httpcode, httpstr))

            success = False
            # if the transfer was successful
            if httpcode in [200, 201, 301]:
                # if the size needs to be checked
                if checksize:
                    # get the remote and local file sizes
                    if down:
                        info = self.webdav.ls(sfile)
                        fs1 = info[0].size
                        fs = os.stat(dfile).st_size
                    else:
                        fs1 = os.stat(sfile).st_size
                        info = self.webdav.ls(dfile)
                        fs = info[0].size
                    if fs1 != fs:
                        print "Filesizes do not match, expected %i, got %i" % (fs1, fs)
                    else:
                        success = True
                # if the md5sum needs to be checked
                elif checkmd5:
                    if "md5sum" in fdict:
                        if fdict["md5sum"] != dul.get_md5sum_file(dfile):
                            print "md5sums do not match"
                        else:
                            success = True
                    else:
                        print "No md5sum information supplied, cannot compare"
                else:
                    success = True
                sys.stdout.flush()

                if success:
                    break
            # if this was the last attempt then do diagnostics
            if x == numTries - 1:
                # run some diagnostics                                                                                                                                                                                                        
                miscutils.fwdebug_print("*" * 75)
                miscutils.fwdebug_print("WEBDAV %s FAILURE" % mode.upper())
                miscutils.fwdebug_print("webdav %s: %s" % (mode, sfile))
                miscutils.fwdebug_print("http status: %i (%s)" % (httpcode, httpstr))


                print "\nDiagnostics:"
                print "Directory info"
                sys.stdout.flush()
                os.system("pwd; find . -exec ls -ld {} \;")
                print "\nFile system disk space usage"
                sys.stdout.flush()
                os.system("df -h .")

                hostm = re.search(r"https?://([^/:]+)[:/]", src)
                if hostm:
                    hname = hostm.group(1)
                    try:   # don't let exception here halt
                        print "Running commands to %s for diagnostics" % hname
                        print "\nPinging %s" % hname
                        sys.stdout.flush()
                        os.system("ping -c 4 %s" % hname)
                        print "\nRunning traceroute to %s" % hname
                        sys.stdout.flush()
                        os.system("traceroute %s" % hname)
                    except:   # print exception but continue
                        (type, value, trback) = sys.exc_info()
                        traceback.print_exception(type, value, trback, file=sys.stdout)
                        print "\n\nIgnoring remote diagnostics exception.   Continuing.\n"
                else:
                    print "Couldn't find url in source file:", src
                    print "Skipping remote diagnostics.\n"

                print "*" * 75
                sys.stdout.flush()

            if x < numTries - 1:    # not the last time in the loop
                miscutils.fwdebug_print("Sleeping %s secs" % secondsBetweenRetries)
                time.sleep(secondsBetweenRetries)

        if not success:
            msg = "File copy failed with http status %s (%s)" % (httpcode, httpstr)
            raise Exception(msg)

        if not isTest:
            return time.time()-starttime

    def create_http_intermediate_dirs(self, f):
        """ Create all directories that are valid prefixes of the URL *f*.

            Parameters
            ----------
            f : str
                String containing the full desintation path

            >>> C = EwdUtils('test_http_utils/.desservices.ini','file-http', 'desar2.cosmology.illinois.edu'')
            >>> C.create_http_intermediate_dirs("http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh.txt")
        """
        # Making bar/ sometimes returns a 301 status even if there doesn't seem to be a bar/ in the directory.
        m = re.match("(http://[^/]+)(/.*)", f)
        direc = miscutils.get_list_directories([m.group(2)])[-1]
        self.webdav.mkdirs(direc[1:])
        for x in miscutils.get_list_directories([m.group(2)]):
            self.existing_directories.add(x)

    def copyfiles(self, filelist, tstats, secondsBetweenRetriesC=30, numTriesC=5, verify=None):
        """ Copies files in given src,dst in filelist
        
            Parameters
            ----------
            filelist : dict
                Dictionary of the files to transfer along with their peoperties

            tstats : object
              
            secondsBetweenRetriesC : int
                How long to wait between retries of the transfer in seconds
                Default: 30

            numTriesC : int
                Maximum number of times to attempt the file transfer.
                Default: 5

            verify : str
                What type of verification to do ("filesize", "md5sum", None)
                Default: None

            Returns
            ------
            Tuple containing the status (int) and the file list
        """
        num_copies_from_archive = 0
        num_copies_to_archive = 0
        total_copy_time_from_archive = 0.0
        total_copy_time_to_archive = 0.0
        status = 0

        # loop over each file
        for filename, fdict in filelist.items():
            fsize = 0
            if 'filesize' in fdict and fdict['filesize'] is not None:
                fsize = fdict['filesize']
            try:
                (src, isurl_src) = self.check_url(fdict['src'])
                (dst,isurl_dst) = self.check_url(fdict['dst'])
                if (isurl_src and isurl_dst) or (not isurl_src and not isurl_dst):
                    miscutils.fwdie("Exactly one of isurl_src and isurl_dst has to be true (values: %s %s %s %s)"
                          % (isurl_src, src, isurl_dst, dst), fmdefs.FM_EXIT_FAILURE)
                copy_time = None
                # if local file and file doesn't already exist
                if not isurl_dst and not os.path.exists(dst):
                    if tstats is not None:
                        tstats.stat_beg_file(filename)

                    # make the path
                    path = os.path.dirname(dst)
                    if len(path) > 0 and not os.path.exists(path):
                        miscutils.coremakedirs(path)
                    # getting some non-zero curl exit codes, double check path exists
                    if len(path) > 0 and not os.path.exists(path):
                        raise Exception("Error: path still missing after coremakedirs (%s)" % path)
                    copy_time = self.move_file(src, dst, secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC, verify=verify, fdict=fdict)
                    if tstats is not None:
                        tstats.stat_end_file(0, fsize)
                elif isurl_dst:   # if remote file
                    if tstats is not None:
                        tstats.stat_beg_file(filename)
                    # create remote paths
                    self.create_http_intermediate_dirs(dst)
                    copy_time = self.move_file(src, dst, secondsBetweenRetries=secondsBetweenRetriesC, numTries=numTriesC, verify=verify, fdict=fdict)

                    if tstats is not None:
                        tstats.stat_end_file(0, fsize)
                # Print some debugging info:
                if miscutils.fwdebug_check(9, "HTTP_UTILS_DEBUG"):
                    miscutils.fwdebug_print("\n")
                    for lines in traceback.format_stack():
                        for L in lines.split('\n'):
                            if L.strip() != '':
                                miscutils.fwdebug_print("call stack: %s" % L)
                if miscutils.fwdebug_check(3, "HTTP_UTILS_DEBUG"):
                    miscutils.fwdebug_print("Copy info: %s %s %s %s %s %s" % (EwdUtils.copyfiles_called,
                                                                              fdict['filename'],
                                                                              fsize,
                                                                              copy_time,
                                                                              time.time(),
                                                                              'toarchive' if isurl_dst else 'fromarchive'))

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
                    tstats.stat_end_file(1, fsize)
                filelist[filename]['err'] = str(err)
                miscutils.fwdebug_print(str(err))
        if tstats is None:
            print "[Copy summary] copy_batch:%d  file_copies_to_archive:%d time_to_archive:%.3f copies_from_archive:%d time_from_archive:%.3f  end_time_for_batch:%.3f" % \
            (EwdUtils.copyfiles_called, num_copies_to_archive, total_copy_time_to_archive, num_copies_from_archive, total_copy_time_from_archive, time.time())

        EwdUtils.copyfiles_called += 1
        return (status, filelist)

    def test_copyfiles(self):
        """ Method to test the file transfer

        """
        # Keeping these copies in separate lists since otherwise can't ensure the order they
        # would be copied in.
        test_filelist1 = {'testfile_dh.txt': {'compression': None,
                                              'src': './testfile_dh.txt',
                                              'filename': 'testfile_dh.txt',
                                              'filesize': 12,
                                              'path': './',
                                              'dst': 'http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt'}}
        test_filelist2 = {'testfile_dh2.txt': {'compression': None,
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/testfile_dh.txt',
                                               'filename': '20130716_bpm_10.fits',
                                               'filesize': 16781760,
                                               'path': 'refacthome/OPS/cal/bpm/',
                                               'dst': 'test_dh/testfile_dh2.txt'}}
        test_filelist3 = {'testfile_dh3.txt': {'compression': None,
                                               'src': './testfile_dh.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'dst': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt'}}
        test_filelist4 = {'testfile_dh4.txt': {'compression': None,
                                               'dst': 'test_dh/testfile_dh3.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/testfile_dh3.txt'}}
        test_filelist5 = {'testfile_dh5.txt': {'compression': None,
                                               'dst': 'test_dh/testfile_dh5.txt',
                                               'filename': 'testfile_dh.txt',
                                               'filesize': 12,
                                               'path': './',
                                               'src': 'http://desar2.cosmology.illinois.edu/DESTesting/bar/arggdfsbqwr.txt'}}

        shutil.rmtree('test_dh', True)
        with open("testfile_dh.txt", "w") as f: f.write("hello, world")

        self.copyfiles(test_filelist1, None)
        self.copyfiles(test_filelist2, None)
        if os.path.isfile('test_dh/testfile_dh2.txt') and os.path.getsize('test_dh/testfile_dh2.txt') == 12:
            print "Transfer of test_dh2.txt seems ok"
        else:
            miscutils.fwdie("Transfer of test_dh2.txt failed", fmdefs.FM_EXIT_FAILURE)
        self.webdav.delete("DESTesting/testfile_dh.txt")

        self.copyfiles(test_filelist3, None)
        self.copyfiles(test_filelist4, None)
        if os.path.isfile('test_dh/testfile_dh3.txt') and os.path.getsize('test_dh/testfile_dh3.txt') == 12:
            print "Transfer of test_dh3.txt (with an intermediate dir) seems ok"
        else:
            miscutils.fwdie("Transfer of test_dh3.txt (with an intermediate dir) failed", fmdefs.FM_EXIT_FAILURE)
        self.webdav.delete("DESTesting/bar/testfile_dh3.txt")
        self.webdav.delete("DESTesting/bar/")

        (status, filelist5_out) = self.copyfiles(test_filelist5, None, secondsBetweenRetriesC=1, numTriesC=2)
        if filelist5_out['testfile_dh5.txt'].has_key('err') and filelist5_out['testfile_dh5.txt']['err'] == 'File operation failed with return code 22, http status 404.':
            print "Test of failed GET seems ok."
        else:
            miscutils.fwdie("Test of failed GET failed", fmdefs.FM_EXIT_FAILURE)

        os.system("rm -f ./testfile_dh.txt")
        shutil.rmtree('test_dh', True)


if __name__ == "__main__":
    # To run these unit tests first copy .desservices.ini_example to .desservices.ini
    # in test_http_utils/, and set user and passwd. Then use this command:
    #    python ./http_utils.py -v
    import doctest
    doctest.testmod()
    C = EwdUtils('test_http_utils/.desservices.ini', 'file-http', 'desar2.cosmology.illinois.edu')
    C.test_copyfiles()
    # miscutils.fwdie("Test miscutils.fwdie", fmdefs.FM_EXIT_FAILURE)
