# $Id: utils.py 48549 2019-05-20 19:27:44Z friedel $
# $Rev:: 48549                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:27:44 #$:  # Date of last commit.

import despymisc.miscutils as miscutils
import os
import subprocess
from stat import S_IMODE, S_ISDIR
import pwd
import grp
import datetime

CHUNK = 1024.
UNITS = {0 : 'b',
         1 : 'k',
         2 : 'M',
         3 : 'G',
         4 : 'T',
         5 : 'P'}



##################################################################################################
def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, st in keylist.items():
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif st.lower() == 'req':
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', '******************************')
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'keylist = %s' % keylist)
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'archive_info = %s' % archive_info)
            miscutils.fwdebug(0, 'FMUTILS_DEBUG', 'config = %s' % config)
            miscutils.fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info


def convert_permissions(perm):
    if isinstance(perm, (int, long)):
        perm = str(perm)
    lead = 0
    output = ''
    if len(perm) == 4:
        lead = int(perm[0])
        perm = perm[1:]
    for p in perm:
        x = int(p)
        if x > 3:
            output += 'r'
            x -= 4
        else:
            output += '-'
        if x > 1:
            output += 'w'
            x -= 2
        else:
            output += '-'
        if x > 0:
            output += 'x'
        else:
            output += '-'
    if lead == 2:
        if output[5] == 'x':
            output[5] = 's'
        else:
            output[5] = 'S'
    elif lead == 4:
        if output[2] == 'x':
            output[2] = 's'
        else:
            output[2] = 'S'
    return output


def ls_ld(fname):
    try:
        st = os.stat(fname)
        info = pwd.getpwuid(st.st_uid)
        user = info.pw_name
        group = grp.getgrgid(info.pw_gid).gr_name
        perm = convert_permissions(oct(S_IMODE(st.st_mode)))
        if S_ISDIR(st.st_mode) > 0:
            perm = 'd' + perm
        else:
            perm = '-' + perm
        ctime = datetime.datetime.fromtimestamp(st.st_ctime)
        if datetime.datetime.now() - ctime > datetime.timedelta(180):
            fmt = '%b %d %Y'
        else:
            fmt = '%b %d %H:%M'
        timestamp = ctime.strftime(fmt)
        return "%s  %s %s %-12s %-13s %s" % (perm, user, group, str(st.st_size), timestamp, fname)
    except:
        stat = subprocess.Popen('pwd; ls -ld %s' % (fname), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = stat.communicate()[0]
        return stdout


def find_ls(base):
    for root, dirs, files in os.walk(base):
        for d in dirs:
            print ls_ld(os.path.join(root, d))
        for f in files:
            print ls_ld(os.path.join(root, f))


def get_mount_point(pathname):
    "Get the mount point of the filesystem containing pathname"
    pathname= os.path.normcase(os.path.realpath(pathname))
    parent_device= path_device= os.stat(pathname).st_dev
    while parent_device == path_device:
        mount_point= pathname
        pathname= os.path.dirname(pathname)
        if pathname == mount_point: break
        parent_device= os.stat(pathname).st_dev
    return mount_point

def get_mounted_device(pathname):
    "Get the device mounted at pathname"
    # uses "/proc/mounts"
    pathname= os.path.normcase(pathname) # might be unnecessary here
    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields= line.rstrip('\n').split()
                # note that line above assumes that
                # no mount points contain whitespace
                if fields[1] == pathname:
                    return fields[0]
    except EnvironmentError:
        raise
    return None # explicit

def reduce(size):
    count = 0
    while size > CHUNK:
        size /= CHUNK
        count += 1
    return "%i%s" % (int(size), UNITS[count])

def get_fs_space(pathname):
    "Get the free space of the filesystem containing pathname"
    stat= os.statvfs(pathname)
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
    free = stat.f_bfree * stat.f_bsize
    total = stat.f_blocks * stat.f_bsize
    used = total - free
    return total, free, used

def df_h(path):
    mount = get_mount_point(path)
    dev = get_mounted_device(mount)
    total, free, used = get_fs_space(path)
    percent = int(100. * used/total)
    print 'Filesystem      Size   Used   Avail  Use%  Mounted on'
    print '%-15s %s   %s   %s   %i%%   %s' % (dev, reduce(total), reduce(used), reduce(free), percent, mount)
