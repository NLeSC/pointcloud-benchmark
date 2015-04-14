#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import utils

#
# This module contains methods that use PostgreSQL
#

def mogrifyExecute(self, cursor, query, queryArgs = None):
    """ Execute a query with logging"""
    if queryArgs != None:
        logging.info(cursor.mogrify(query, queryArgs))
        cursor.execute(query, queryArgs)
    else:
        logging.info(cursor.mogrify(query))
        cursor.execute(query)
    cursor.connection.commit()
    
def dropTable(self, cursor, tableName, check = False):
    """ Drops a table"""
    delete = True
    if check:
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName, ))
        if len(cursor.fetchone()[0]) == 0:
            delete = False
    if delete:
        mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
    
def getConnectString(dbName = None, userName= None, password = None, dbHost = None, dbPort = None, cline = False):
    """ Gets the connection string to be used by psycopg2 (if cline is False)
    or by psql (if cline is True)"""
    connString=''
    if cline:    
        if dbName != None and dbName != '':
            connString += " " + dbName
        if userName != None and userName != '':
            connString += " -U " + userName
        if password != None and password != '':
            os.environ['PGPASSWORD'] = password
        if dbHost != None and dbHost != '':
            connString += " -h " + dbHost
        if dbPort != None and dbPort != '':
            connString += " -p " + dbPort
    else:
        if dbName != None and dbName != '':
            connString += " dbname=" + dbName
        if userName != None and userName != '':
            connString += " user=" + userName
        if password != None and password != '':
            connString += " password=" + password
        if dbHost != None and dbHost != '':
            connString += " host=" + dbHost
        if dbPort != None and dbPort != '':
            connString += " port=" + dbPort
    return connString

def getSizes(cursor):
    """ Return tuple with size of indexes, size excluding indexes, and total size (all in MB)"""
    cursor.execute("""SELECT sum(pg_indexes_size(tablename::text)) / (1024*1024) size_indexes,  sum(pg_table_size(tablename::text)) / (1024*1024) size_ex_indexes, sum(pg_total_relation_size(tablename::text)) / (1024*1024) size_total FROM pg_tables where schemaname='public'""")
    return list(cursor.fetchone())

def createSQLFile(cursor, absPath, query, queryArgs):
    sqlFile = open(absPath, 'w')
    sqlFile.write(cursor.mogrify(query, queryArgs) + ';\n')
    sqlFile.close()
    
def executeSQLFileCount(connectionString, sqlFileAbsPath):
    command = 'psql ' + connectionString + ' < ' + sqlFileAbsPath + ' | wc -l'
    result = utils.shellExecute(command).replace('\n','')
    try:
        result  = int(result) - 4
    except:
        result = None
    return result