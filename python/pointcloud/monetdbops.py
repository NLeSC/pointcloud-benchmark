#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud import utils

#
# This module contains methods that use MonetDB
#

def mogrify(cursor, query, queryArgs = None):
    if queryArgs == None:
        return query
    else:
        pquery = query
        for qa in queryArgs:
            qindex = pquery.index('%s')
            pquery = pquery[:qindex] + str(qa) + pquery[qindex+2:]
        return pquery

def mogrifyExecute(cursor, query, queryArgs = None):
    logging.info(mogrify(cursor, query, queryArgs))
    if queryArgs != None:
        return cursor.execute(query, queryArgs)
    else:
        return cursor.execute(query)
 
def dropTable(cursor, tableName, check = False):
    toDelete = True
    if check:
        if not cursor.execute('select name from tables where name = %s', (tableName,)):
            toDelete = False
    if toDelete:
        mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        cursor.connection.commit()

def getSizes(cursor):
    cursor.execute("""select cast(sum(imprints) AS double)/(1024.*1024.), cast(sum(columnsize) as double)/(1024.*1024.), (cast(sum(imprints) AS double)/(1024.*1024.) + cast(sum(columnsize) as double)/(1024.*1024.)) from storage()""")
    return list(cursor.fetchone())