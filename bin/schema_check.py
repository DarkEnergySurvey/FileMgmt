#!/usr/bin/env python

from despydb import desdbi
import argparse

if __name__ == '__main__':
    # program to compare entries between specific tables in different schemas
    parser = argparse.ArgumentParser(description='check specified tables for discrepencies')
    parser.add_argument('--section1', action='store', required=True, help='The section in the des services file for the first schema')
    parser.add_argument('--section2', action='store', help='The section in the des servies file for the second schema (only if different from the first')
    parser.add_argument('--schema1', action='store', required=True, help='The first schema to compare')
    parser.add_argument('--schema2', action='store', required=True, help='The second schema to compare')
    parser.add_argument('--des_services', help='desservices file')
    parser.add_argument('--tables', action='store', default='ops_file_header,ops_metadata,ops_filetype,ops_filetype_metadata,ops_datafile_table,ops_datafile_metadata', help='The tables to compare')
    parser.add_argument('--keys', action='store', default='name,file_header_name,filetype,filetype/file_header_name,filetype,filetype/attribute_name', help='The primary key(s) to use, one for each table, there can be two primary keys for a table, separated by a /')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    # get the list of tables
    tables = args['tables'].split(',')
    #get the list of keys
    keys = args['keys'].split(',')

    # connect to the database
    dbh1 = desdbi.DesDbi(args['des_services'], args['section1'])
    if args['section2']:
        dbh2 = desdbi.DesDbi(args['des_services'], args['section2'])
    else:
        dbh2 = dbh1

    # go over each table
    for num, table in enumerate(tables):
        print "Checking table %s" % (table.upper())
        cur1 = dbh1.cursor()
        cur2 = dbh2.cursor()
        # get the column names
        cur1.execute("select column_name from all_tab_cols where table_name='%s' and owner='%s'" % (table.upper(), args['schema1'].upper()))
        results = cur1.fetchall()
        columns = []
        for res in results:
            columns.append(res[0])
        cur2.execute("select column_name from all_tab_cols where table_name='%s' and owner='%s'" % (table.upper(), args['schema2'].upper()))
        results = cur2.fetchall()
        columns2 = []
        for res in results:
            columns2.append(res[0])

        # make sure columns match in both tables
        if len(columns) != len(columns2):
            print "Different number of columns for table %s: %s has %i and %s has %i" % (table, args['schema1'], len(columns), args['schema2'], len(columns2))
            continue
        hit = False
        for c in columns:
            if not c in columns2:
                print "  %s is missing column %s" % (args['schema2'], c)
                hit = True
        for c in columns2:
            if not c in columns:
                print "  %s is missing column %s" % (args['schema1'], c)
                hit = True

        if hit:
            continue
        # get the data
        colstring =  ",".join(columns)
        cur1.execute('select %s from %s.%s' % (colstring, args['schema1'], table))
        cur2.execute('select %s from %s.%s' % (colstring, args['schema2'], table))
        results1 = cur1.fetchall()
        results2 = cur2.fetchall()

        # sort the data into dictionaries
        data1 = []
        data2 = []
        for res in results1:
            r = {}
            for i, d in enumerate(columns):
                r[d] = res[i]
            data1.append(r)
        for res in results2:
            r = {}
            for i, d in enumerate(columns):
                r[d] = res[i]
            data2.append(r)
        
        onlys1 = []
        onlys2 = []
        diff = []

        # if there are two primary keys
        if '/' in keys[num]:
            key1, key2 = keys[num].split('/')
            key1 = key1.upper()
            key2 = key2.upper()
            for d1 in data1:
                n = None
                for i, d2 in enumerate(data2):
                    if d1[key1] == d2[key1] and d1[key2] == d2[key2]:
                        n = i
                        break
                if n is None:
                    onlys1.append([d1[key1],d1[key2]])
                    continue
                for c in columns:
                    try: # in case they are different data types
                        if d1[c] != d2[c]:
                            diff.append([d1[key1],d1[key2]])
                            break
                    except:
                        diff.append([d1[key1],d1[key2]])
                        break
            if len(onlys1) > 0:
                print "  Entries only in %s, keys %s and %s" % (args['schema1'], key1, key2)
                for i in onlys1:
                    print "    %s  %s" % (str(i[0]), str(i[1]))
                print "\n"
            if len(onlys2) > 0:
                print "  Entries only in %s, keys %s and %s" % (args['schema2'], key1, key2)
                for i in onlys2:
                    print "    %s  %s" % (str(i[0]), str(i[1]))
                print "\n"
            if len(diff) > 0:
                print "  Entries that differ, keys %s and %s" % (key1, key2)
                for i in diff:
                    print "    %s  %s" % (str(i[0]), str(i[1]))
                print "\n"
            if len(onlys1) == 0 and len(onlys1) == 0 and len(diff) == 0:
                print "  All data match\n"

        else: # only 1 primary key
            key = keys[num].upper()
            for d1 in data1:
                n = None
                for i, d2 in enumerate(data2):
                    if d1[key] == d2[key]:
                        n = i
                        break
                if n is None:
                    onlys1.append(d1[key])
                    continue
                for c in columns:
                    try: # in case they are different data types
                        if d1[c] != d2[c]:
                            diff.append(d1[key])
                            break
                    except:
                        diff.append(d1[key])
                        break
            if len(onlys1) > 0:
                print "  Entries only in %s, key %s" % (args['schema1'], key)
                for i in onlys1:
                    print "    %s" % (str(i))
                print "\n"
            if len(onlys2) > 0:
                print "  Entries only in %s, key %s" % (args['schema2'], key)
                for i in onlys2:
                    print "    %s" % (str(i))
                print "\n"
            if len(diff) > 0:
                print "  Entries that differ, key %s" % (key)
                for i in diff:
                    print "    %s" % (str(i))
                print "\n"
            if len(onlys1) == 0 and len(onlys1) == 0 and len(diff) == 0:
                print "  All data match\n"
