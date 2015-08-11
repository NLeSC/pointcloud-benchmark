#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging, cx_Oracle
from pointcloud import utils

#
# This module contains methods that use MonetDB
#

def getConnectString(dbName = None, userName= None, password = None, dbHost = None, dbPort = None):
    """ Gets a connection string to be used in SQLPlus or CxOracle"""
    return userName + "/" + password + "@//" + dbHost + ":" + dbPort + "/" + dbName

def createUser(cursorSuper, userName, password, tableSpace, tempTableSpace):
    """ Creates a user in Oracle"""
    cursorSuper.execute('select * from all_users where username = :1',[userName,])
    if len(cursorSuper.fetchall()):
        cursorSuper.execute('drop user ' + userName + ' CASCADE')
        cursorSuper.connection.commit()
    tsString = ''
    if tableSpace != None and tableSpace != '':
        tsString += ' default tablespace ' + tableSpace + ' '
    if tempTableSpace != None and tempTableSpace != '':
        tsString += ' temporary tablespace ' + tempTableSpace + ' '

    cursorSuper.execute('create user ' + userName + ' identified by ' + password + tsString)
    cursorSuper.execute('grant unlimited tablespace, connect, resource, create view to ' + userName)
    cursorSuper.connection.commit()

def createDirectory(cursorSuper, directoryVariableName, directoryAbsPath, userName):
    """ Creates a Oracle directory with read permission for the user"""
    cursorSuper.execute('select directory_name from all_directories where directory_name = :name', [directoryVariableName,])
    if len(cursorSuper.fetchall()):
        cursorSuper.execute("drop directory " + directoryVariableName)
        cursorSuper.connection.commit()
    cursorSuper.execute("create directory " + directoryVariableName + " as '" + directoryAbsPath + "'")
    cursorSuper.execute("grant read on directory " + directoryVariableName + " to " + userName)
    cursorSuper.connection.commit()

def mogrify(cursor, query, queryArgs = None):
    """ Logs the query statement and execute it"""
    query = query.upper()
    if queryArgs == None:
        return query
    else:
        cursor.prepare(query)
        bindnames = cursor.bindnames()
        if len(queryArgs) != len(bindnames):
            raise Exception('Error: len(queryArgs) != len(bindnames) \n ' + str(queryArgs) + '\n' + str(bindnames))
        if (type(queryArgs) == list) or (type(queryArgs) == tuple):
            for i in range(len(queryArgs)):
                query = query.replace(':'+bindnames[i],str(queryArgs[i]))
            return query
        elif type(queryArgs) == dict:
            upQA = {}
            for k in queryArgs:
                upQA[k.upper()] = queryArgs[k]
            for bindname in bindnames:
                query = query.replace(':'+bindname, str(upQA[bindname]))
            return query
        else:
            raise Exception('Error: queryArgs must be dict, list or tuple')

def mogrifyExecute(cursor, query, queryArgs = None):
    """ Execute a query with logging"""
    logging.info(mogrify(cursor, query, queryArgs))
    if queryArgs != None:
        cursor.execute(query, queryArgs)
    else:
        cursor.execute(query)
    cursor.connection.commit()
        
def dropTable(cursor, tableName, check = False):
    """ Drops a table"""
    if check:
        cursor.execute('SELECT table_name FROM all_tables WHERE table_name = :1',[tableName,])
        if len(cursor.fetchall()):
            mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
    else:
        mogrifyExecute(cursor, 'DROP TABLE ' + tableName)
        
def getSizeTable(cursor, tableName):
    """ Get the size in MB of a table"""
    try:
        if type(tableName) == str:
            tableName = [tableName, ]
        
        queryArgs = {}
        segs = []
        tabs = []
        for i in range(len(tableName)):
            name = 'name' + str(i)
            queryArgs[name] = tableName[i] + '%'
            segs.append('segment_name LIKE :' + name)
            tabs.append('table_name LIKE :' + name)
        
        cursor.execute("""
SELECT sum(bytes/1024/1024) size_in_MB FROM user_segments
WHERE (""" + ' OR '.join(segs) + """
OR segment_name in (
SELECT segment_name FROM user_lobs
WHERE """ + ' OR '.join(tabs) + """
UNION
SELECT index_name FROM user_lobs
WHERE """ + ' OR '.join(tabs) + """
)       
)""", queryArgs)
        
        size = cursor.fetchall()[0][0]
        if size == None:
            size = 0
    except:
        size = 0
    return size
 
def getSizeUserIndexes(cursor):
    """ Get the size of the user indexes"""
    try:
        cursor.execute("select sum(leaf_blocks * 0.0078125) from USER_INDEXES")
        size = cursor.fetchall()[0][0]
        if size == None:
            size = 0
    except:
        size = 0
    return size

def getSizeUserSDOIndexes(cursor, tableName):
    """ Get the size of the spatial indexes related to a table"""
    try:
        cursor.callproc("dbms_output.enable")
        q = """
DECLARE
size_in_mb  number;
idx_tabname varchar2(32);
BEGIN
dbms_output.enable;
SELECT sdo_index_table into idx_tabname FROM USER_SDO_INDEX_INFO
where table_name = :name and sdo_index_type = 'RTREE';
execute immediate 'analyze table '||idx_tabname||' compute system statistics for table';
select blocks * 0.0078125 into size_in_mb from USER_TABLES where table_name = idx_tabname;
dbms_output.put_line (to_char(size_in_mb));
END;
    """
        cursor.execute(q, [tableName,])
        statusVar = cursor.var(cx_Oracle.NUMBER)
        lineVar = cursor.var(cx_Oracle.STRING)
        size = float(cursor.callproc("dbms_output.get_line", (lineVar, statusVar))[0])
    except:
        size = 0
    return size

def createSQLFile(cursor, absPath, query, queryArgs):
    sqlFile = open(absPath, 'w')
    sqlFile.write('set linesize 120;\n')
    sqlFile.write('set trimout on;\n')
    sqlFile.write('set pagesize 0;\n')
    sqlFile.write('set feedback off;\n')
    sqlFile.write(mogrify(cursor, query, queryArgs) + ';\n')
    sqlFile.close()
    
def executeSQLFileCount(connectionString, sqlFileAbsPath):
    command = 'sqlplus -s ' + connectionString + ' < ' + sqlFileAbsPath + ' | wc -l'
    result = utils.shellExecute(command).replace('\n','')
    try:
        result  = int(result)
    except:
        result = -1
    return result
