#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging, subprocess
from pointcloud import utils

#
# This module contains methods that use MonetDB
#

def mogrify(cursor, query, queryArgs = None):
    """ Returns a string representation of the query statement"""
    if queryArgs == None:
        return query
    else:
        pquery = query
        for qa in queryArgs:
            qindex = pquery.index('%s')
            pquery = pquery[:qindex] + str(qa) + pquery[qindex+2:]
        return pquery

def mogrifyExecute(cursor, query, queryArgs = None):
    """ Execute a query with logging"""
    logging.info(mogrify(cursor, query, queryArgs))
    if queryArgs != None:
        return cursor.execute(query, queryArgs)
    else:
        return cursor.execute(query)
    cursor.connection.commit()
    
def dropTable(cursor, tableName, check = False):
    """ Drops a table"""
    toDelete = True
    if check:
        if not cursor.execute('select name from tables where name = %s', (tableName,)):
            toDelete = False
    if toDelete:
        mogrifyExecute(cursor, 'DROP TABLE ' + tableName)

def getSizes(cursor):
    """ Get the sizes of the DB (indexes, tables, indexes+tables)"""
    cursor.execute("""select cast(sum(imprints) AS double)/(1024.*1024.), cast(sum(columnsize) as double)/(1024.*1024.), (cast(sum(imprints) AS double)/(1024.*1024.) + cast(sum(columnsize) as double)/(1024.*1024.)) from storage()""")
    return list(cursor.fetchone())

def createSQLFile(absPath, query, queryArgs):
    sqlFile = open(absPath, 'w')
    sqlFile.write(mogrify(None, query, queryArgs) + ';\n')
    sqlFile.close()
    
def executeSQLFileCount(connectionString, sqlFileAbsPath):
    command = 'mclient ' + connectionString + ' < ' + sqlFileAbsPath + ' | wc -l'
    result = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].replace('\n','')
    try:
        result  = int(result) - 5
    except:
        result = None
    return result