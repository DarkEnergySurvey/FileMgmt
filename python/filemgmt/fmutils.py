# $Id: fmutils.py 46644 2018-03-12 19:54:58Z friedel $
# $Rev:: 46644                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-03-12 14:54:58 #$:  # Date of last commit.

""" Miscellaneous FileMgmt utils """

import despymisc.miscutils as miscutils
import despymisc.misctime as misctime
import json


##################################################################################################
def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, stat in keylist.items():
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            miscutils.fwdebug_print('******************************')
            miscutils.fwdebug_print('keylist = %s' % keylist)
            miscutils.fwdebug_print('archive_info = %s' % archive_info)
            miscutils.fwdebug_print('config = %s' % config)
            miscutils.fwdie('Error: Could not find required key (%s)' % k, 1, 2)
    return info

######################################################################
def read_json_single(json_file, allMandatoryExposureKeys):
    """ Reads json manifest file """

    if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
        miscutils.fwdebug_print("reading file %s" % json_file)

    allExposures = []

    my_header = {}
    numseq = {}
    all_exposures = dict()
    with open(json_file) as my_json:
        for line in my_json:
            all_data = json.loads(line)

            for key, value in all_data.items():
                if key == 'header':
                    #read the values for the header (date and set_type are here)
                    my_head = value

                    allExposures.append(str(my_head['set_type']))
                    allExposures.append(str(my_head['createdAt']))

                if key == 'exposures':
                    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                        miscutils.fwdebug_print("line = exposures = %s" % value)
                    #read all the exposures that were taken for the set_type in header
                    my_header = value

                    #Total Number of exposures in manifest file
                    tot_exposures = len(my_header)

                    if tot_exposures is None or tot_exposures == 0:
                        raise Exception("0 SN exposures parsed from json file")

                    for i in range(tot_exposures):
                        if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                            miscutils.fwdebug_print("Working on exposure %s" % (i))
                            miscutils.fwdebug_print("\texpid = %s" % my_header[i]['expid'])
                            miscutils.fwdebug_print("\tdate = %s" % my_header[i]['date'])
                            miscutils.fwdebug_print("\tacttime = %s" % my_header[i]['acttime'])
                        if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                            miscutils.fwdebug_print("Entire exposure %s = %s" % (i, my_header[i]))

                        numseq = my_header[i]['sequence']
                        mytime = my_header[i]['acttime']
                        #if mytime > 10 and numseq['seqnum'] == 2:
                        #    first_expnum = my_header[i]['expid']

                        #Validate if acctime has a meaningful value.
                        #If acttime = 0.0, then it's a bad exposure. Skip it from the manifest.
                        if mytime == 0.0:
                            continue
                        try:
                            for mandatoryExposureKey in allMandatoryExposureKeys:
                                if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                                    miscutils.fwdebug_print("mandatory key %s" % \
                                                            mandatoryExposureKey)
                                key = str(mandatoryExposureKey)

                                if my_header[i][mandatoryExposureKey]:
                                    if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                                        miscutils.fwdebug_print("mandatory key '%s' found %s" % \
                                                                (mandatoryExposureKey,
                                                                 my_header[i][mandatoryExposureKey]))
                                    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                                        miscutils.fwdebug_print("allExposures in for: %s" % allExposures)

                                    try:
                                        if key == 'acttime':
                                            key = 'EXPTIME'
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                        elif key == 'filter':
                                            key = 'BAND'
                                            all_exposures[key].append(str(my_header[i][mandatoryExposureKey]))
                                        elif key == 'expid':
                                            key = 'EXPNUM'
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                        else:
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                    except KeyError:
                                        all_exposures[key] = [my_header[i][mandatoryExposureKey]]


                        except KeyError:
                            miscutils.fwdebug_print("Error: missing key '%s' in json entity: %s " % \
                                                    (mandatoryExposureKey, my_header[i]))
                            raise

                    if len(all_exposures) == 0:
                        raise ValueError("Found 0 non-pointing exposures in manifest file")

                    timestamp = all_exposures['date'][0]
                    nite = misctime.convert_utc_str_to_nite(timestamp)

                    # get field by parsing set_type
                    #print 'xxxx', my_head['set_type']
                    myfield = my_head['set_type']
                    if len(myfield) > 5:
                        newfield = myfield[:5]
                    else:
                        newfield = myfield

                    camsym = 'D'   # no way to currently tell CAMSYM/INSTRUME from manifest file

                    if not newfield.startswith('SN-'):
                        raise ValueError("Invalid field (%s).  set_type = '%s'" % (newfield, my_head['set_type']))

                    #if json_file contains a path or compression extension, then cut it to only the filename
                    jsonfile = miscutils.parse_fullname(json_file, miscutils.CU_PARSE_FILENAME)

                    if tot_exposures is None or tot_exposures == 0:
                        raise Exception("0 SN exposures parsed from json file")

                    for i in range(tot_exposures):
                        if my_header[i]['acttime'] == 0.0:
                            continue
                        if i == 0:
                            #all_exposures['FIELD'] = [str(my_head['set_type'])]
                            all_exposures['FIELD'] = [newfield]
                            all_exposures['CREATEDAT'] = [str(my_head['createdAt'])]
                            all_exposures['MANIFEST_FILENAME'] = [jsonfile]
                            all_exposures['NITE'] = [nite]
                            all_exposures['SEQNUM'] = [1]
                            all_exposures['CAMSYM'] = [camsym]
                        else:
                            #all_exposures['FIELD'].append(str(my_head['set_type']))
                            all_exposures['FIELD'].append(newfield)
                            all_exposures['CREATEDAT'].append(str(my_head['createdAt']))
                            all_exposures['MANIFEST_FILENAME'].append(jsonfile)
                            all_exposures['NITE'].append(nite)
                            all_exposures['SEQNUM'].append(1)
                            all_exposures['CAMSYM'].append(camsym)

    # Add the manifest filename value in the dictionary
    #all_exposures['MANIFEST_FILENAME'] = json_file
    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
        miscutils.fwdebug_print("allExposures " % (all_exposures))

    return all_exposures

##################################################################################################

class DataObject(object):
    """ Class to turn a dictionary into class elements

    """
    def __init__(self, **kw):
        for item, val in kw.iteritems():
            setattr(self, item, val)

    def get(self, attrib):
        """ Method to get the value of the given attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to get

            Returns
            -------
            The value of the attribute

        """
        return getattr(self, attrib, None)

    def set(self, attrib, value):
        """ Method to set the value of an attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to set

            value : vaires
                The value to set the attribute to

        """
        if not hasattr(self, attrib):
            raise Exception("%s is not a member of DataObject." % (attrib))
        setattr(self, attrib, value)
