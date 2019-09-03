#!/usr/bin/env python
# $Id: arcp.py 41008 2015-12-11 15:55:43Z mgower $
# $Rev:: 41008                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:55:43 #$:  # Date of last commit.

""" TODO docstring """

# TODO postponing what if single site copy between two archives on single site?  We shouldn't have this use case.

import sys
import argparse

import filemgmt.filemgmtdb as filemgmtdb
import despymisc.miscutils as miscutils


class arcp(object):
    def __init__ (self, src, dst, config, argv):
        self.argv = argv

        if 'archive' not in config:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError('config missing archive')

        if 'archive_val' not in config:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError('config missing archive_val')

        if 'archive_transfer' not in config:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError('config missing archive_transfer')

        if 'archive_transfer_opt' not in config:
            miscutils.fwdebug(2, "ARCP_DEBUG", "warning archive_transfer_opt is not in config")
        self.config = config

        if src not in config['archive']:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError('%s is not valid archive name' % src)
        self.src = src

        if dst not in config['archive']:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError('%s is not valid archive name' % dst)
        self.dst = dst

        err = False
        if src in config['archive_transfer']:
            if dst not in config['archive_transfer'][src]:
                err = True
            else:
                self.transfer_class = config['archive_transfer'][src][dst]
        elif dst in config['archive_transfer']:    # use reverse direction transfer specs
            if src not in config['archive_transfer'][dst]:
                err = True
            else:
                self.transfer_class = config['archive_transfer'][dst][src]
        else:
            err = True
        
        if err:
            miscutils.fwdebug(2, "ARCP_DEBUG", "config = %s", config)
            raise KeyError("'missing entry in config['archive_transfer'] for %s,%s" % (src,dst))

    def get_list_filenames(self, dbh, args):
        """ Get a lit of filenames whose specs are defined inputs to argparse """
        # args is an array of "command-line" args possibly with keys for query
        # returns python list of filenames

        # get metatable's columns to parse from args
        parser = argparse.ArgumentParser(description='get_src_file_path')
        parser.add_argument('--metatable', action='store', default=None)
        myargs, unknown = parser.parse_known_args(args)
        myargs = vars(myargs)
        if myargs['metatable'] is None:
            miscutils.fwdie("Error: must specify either list or metatable", 1)
        metatable = myargs['metatable']

        # parse all args for column keys in order to build query
        colnames = self.get_column_names(metatable);
        parser = argparse.ArgumentParser(description='get_src_file_path')
        for c in colnames:
            parser.add_argument('--%s' % c, action='store', default=None)
        myargs, unknown = parser.parse_known_args(args)
        myargs = vars(myargs)
        key_vals = { k : myargs[k] for k in myargs if myargs[k] != None }

        # create query string to get filenames
        querydict = {metatable: {'select_fields': ['filename'], 'key_vals': key_vals}}
        querystr = queryutils.create_query_string(dbh, querydict)
        curs = self.cursor()
        curs.execute(querystr)
        filelist = []
        for line in curs:
            filelist.append(line[0])

        return filelist

   add_file_list(filelist=None):
        if filelist is None:
            # dynamically load class for filemgmt
            filemgmt = None
            try:
                miscutils.fwdebug(3, 'ARCP_DEBUG', "filemgmt_class = %s" % filemgmt_class)
                modparts = filemgmt_class.split('.')
                fromname = '.'.join(modparts[0:-1])
                importname = modparts[-1]
                miscutils.fwdebug(3, 'ARCP_DEBUG', "\tfromname = %s" % fromname)
                miscutils.fwdebug(3, 'ARCP_DEBUG', "\timportname = %s" % importname)
                mod = __import__(fromname, fromlist=[importname])
                filemgmt_class = getattr(mod, importname)
                filemgmt = filemgmt_class(self.argv)
            except Exception as err:
                print "ERROR\nError: creating filemgmt object\n%s" % err
                raise
    
            filelist = fmdb.get_list_filenames(self.argv)

        # should this raise error if empty filelist?

        self.filelist = filelist
        
        miscutils.fwdebug(2, "ARCP_DEBUG", "filelist = %s", filelist)




    def transfer(self):
        
        # create class_filesys object, passing it all args 
        # create class_filemgmt object, passing it all args

        filemgmt_class = self.config['archive'][self.src]['filemgmt']
        if filemgmt_class is None or '.' not in filemgmt_class:
            print "Error: Invalid filemgmt class name (%s)" % filemgmt_class
            print "\tMake sure it contains at least 1 period."
            miscutils.fwdie("Invalid filemgmt class name", 1)


    
        filelist = fmdb.add_src_file_path(src_archive_name, dbconfig, filelist, allargs)
        print "----- filelist -----"
        print filelist
    
        fmdb.add_dest_file_path(dest_archive_name, dbconfig, filelist, allargs)
        print "----- filelist -----"
        print filelist
    
        # call class_filesys.copy (responsible for making any dest directories)
        transfer_class = self.config['']



###############################################################################
def get_config_from_db(argv)
    parser = argparse.ArgumentParser(description='get_config_from_db')
    parser.add_argument('--desservices', action='store')
    parser.add_argument('--section', action='store')
    args, unknownargs = parser.parse_known_args(argv)
    args = vars(args)  # make a dictionary

    desservices = None
    if 'desservices' in args:
        desservices = args['desservices']

    section = None
    if 'section' in args:
        section = args['section']

    qdict = OrderedDict()
    import despydb.desdbi as desdbi
    with desdbi.DesDbi(desservices, section) as dbh:
        (qdict['archive'], qdict['archive_val'])  = dbh.get_archive_info()
        (qdict['archive_transfer'], qdict['archive_transfer_opt']).dbh.get_archive_transfer()

    #pretty_print_dict(qdict, None, True)
    return qdict

#srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']

        # get archive path for files from src, create src path and dst path
        # dynamically load filemgmt class for src
        srcfilemgmt_class = dynamically_load_class(self.config['src_archive_info']['filemgmt'])
        needed_vals = srcfilemgmt_class.needed_config_vals()
        configDict = OrderedDict()
        if needed_vals is not None:
            for nv in needed_vals:
                configDict[nv] = self.config[nv]

        # if have them in config, always include DB connection info just in case
        if 'des_db_section' in self.config:
            configDict['des_db_section'] = self.config['des_db_section']
        if 'des_services' in config:
            configDict['des_services'] = self.config['des_services']

        srcfilemgmt = filemgmt_class(configDict=configDict)

        src_rel_locations = srcfilemgmt.get_file_location(filelist, src_archive_info['name'], FM_PREFER_COMPRESSED)
        missing_files = set(filelist) - set(src_rel_locations.keys())

        # dst rel path is same as src rel path
        for filename, relfilename in src_rel_locations.items()
            files2copy["%s/%s" % (srcroot, relfilename)] = "%s/%s" % (dstroot, relfilename)




        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='arcp.py')
    parser.add_argument('dst', action='store')
    parser.add_argument('src', action='store')
    parser.add_argument('--config', action='store')
    parser.add_argument('--outdfg', action='store')
    parser.add_argument('--use_db', action='store')
    parser.add_argument('--list', '--filelist', action='store', dest='listname', default=None)
    args, unknownargs = parser.parse_known_args(sys.argv[1:])
    args = vars(args)  # make a dictionary

    dbconfig = None
    if use_db(args):
        dbconfig = get_config_from_db(args)

    fileconfig = None
    if 'config' in args:
        import intgutils.wclutils as wclutils
        with open(args['configfile'], 'r') as fh:
            fileconfig = wclutils.read_wcl(fh)


    # precedence - db, file, func arg
    config = OrderedDict()

    if dbconfig:
        config.update(dbconfig)

    if fileconfig:
        config.update(fileconfig)
        

    # if asked to output config
    if 'outcfg' in args:
        import intgutils.wclutils as wclutils
        with open(args['outcfg'], 'w') as fh:
            wclutils.write_wcl(config, fh, True, 4)

    filelist = None
    if 'listname' in args:
        filelist = (line.rstrip('\n') for line in open(listname))

    a = arcp(args['src'], args['dst'], args['config'], unknownargs)
    a.add_file_list(filelist, unknownargs)
    a.transfer()

