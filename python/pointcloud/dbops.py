#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, multiprocessing,numpy
from pointcloud import utils 

# Module containing common code for the DB queriers and loaders

def getSelectCols(columnsKeys, columnsNameDict, statistics = None, cas = False):
    selectColumns = []
    for i in range(len(columnsKeys)):
        columnDBName = columnsNameDict[columnsKeys[i]][0]
        if statistics == None:
            if cas:
                selectColumns.append(columnDBName  + ' as ' + columnsKeys[i])
            else:
                selectColumns.append(columnDBName)
        else:
            selectColumns.append(statistics[i] + '(' + columnDBName + ') as stat' + str(i))
    return ','.join(selectColumns)

def createResultsTable(cursor, executeMethod, tableName, columnsKeys, columnsNameTypeDict, statistics = None):
    cs = []
    for i in range(len(columnsKeys)):
        columnData = columnsNameTypeDict[columnsKeys[i]]
        columnDBName = columnData[0]
        columnDBType = columnData[1]
        if statistics == None:
            cs.append(columnDBName + ' ' + columnDBType)
        else:
            cs.append(statistics[i] + columnDBName + ' ' + columnDBType) 
    executeMethod(cursor,"create table "  + tableName + " (" + ','.join(cs) + ")")
    cursor.connection.commit()
   
def getNumPoints(cursor, tableName):
    try:
        cursor.execute('select count(*) from ' + tableName)
        numpoints = int(cursor.fetchone()[0])
    except:
        numpoints = -1
        cursor.connection.rollback()
    return numpoints

def getStatistics(cursor, tableName, columnsNameDict, preComputed = True, columnsKeys = None, statistics = None):
    try:
        if preComputed:
            cursor.execute('select * from ' + tableName)
        else:
            cursor.execute('select ' + getSelectCols(columnsKeys, columnsNameDict, statistics) + ' from ' + tableName)
        stats = list(cursor.fetchone())
        for i in range(len(stats)):
            stats[i] = str(stats[i])
        result = ','.join(stats)
    except:
        result = '-'
        cursor.connection.rollback()
    return result

def getResult(cursor, t0, tableName, columnsNameDict, preComputed = True, columnsKeys = None, statistics = None):
    if statistics != None:
        result = getStatistics(cursor, tableName, columnsNameDict, preComputed, columnsKeys, statistics)
        eTime = time.time() - t0
    else:
        eTime = time.time() - t0
        result = getNumPoints(cursor, tableName)
    return (eTime,result)

def addZCondition(queryParameters, zColumnName, queryArgs):
    zconds = []
    if queryParameters.minz != None:
        if queryParameters.hardcode:
            zconds.append(" " + zColumnName + " >= " + str(queryParameters.minz) + " ")
        else:
            zconds.append(" " + zColumnName + " >= " + queryParameters.pattern + " ")
            queryArgs.append(queryParameters.minz)
    if queryParameters.maxz != None:
        if queryParameters.hardcode:
            zconds.append(" " + zColumnName + " <= " + str(queryParameters.maxz) + " ")
        else:
            zconds.append(" " + zColumnName + " <= " + queryParameters.pattern + " ")
            queryArgs.append(queryParameters.maxz)
    return ' AND '.join(zconds) 

def addMortonCondition(queryParameters, mortonRanges, mortonColumnName, queryArgs):
    elements = []
    for mortonRange in mortonRanges:
        if queryParameters.hardcode:
            elements.append('(' + mortonColumnName + ' between ' + str(mortonRange[0]) + ' and ' + str(mortonRange[1]) + ')')
        else:
            elements.append('(' + mortonColumnName + ' between ' + queryParameters.pattern + ' and ' + queryParameters.pattern + ')')
            queryArgs.extend(mortonRange)
    if len(elements):
        return '(' + ' OR '.join(elements) + ')'
    return None

def addBBoxCondition(queryParameters, xColumnName, yColumnName, queryArgs):   
    if queryParameters.hardcode:
        return "(" + xColumnName + " between " + str(queryParameters.minx) + " and " + str(queryParameters.maxx) + ") and (" + yColumnName + " between " + str(queryParameters.miny) + " and " + str(queryParameters.maxy) + ")"
    else:
        queryArgs.extend([queryParameters.minx, queryParameters.maxx, queryParameters.miny, queryParameters.maxy])
        return "(" + xColumnName + " between " + queryParameters.pattern + " and " + queryParameters.pattern + ") and (" + yColumnName + " between " + queryParameters.pattern + " and " + queryParameters.pattern + ")"

def addBBoxCircleCondition(queryParameters, xColumnName, yColumnName, queryArgs):
    if queryParameters.hardcode:
        return "(" + xColumnName + " between " + str(queryParameters.cx) + "-" + str(queryParameters.rad) + " and " + str(queryParameters.cx) + "+" + str(queryParameters.rad) + ") and (" + yColumnName + " between " + str(queryParameters.cy) + "-" + str(queryParameters.rad) + " and " + str(queryParameters.cy) + "+" + str(queryParameters.rad) + ")"
    else:
        queryArgs.extend([queryParameters.cx,queryParameters.rad,queryParameters.cx,queryParameters.rad,queryParameters.cy,queryParameters.rad,queryParameters.cy,queryParameters.rad])
        return "(" + xColumnName + " between " + queryParameters.pattern + "-" + queryParameters.pattern + " and " + queryParameters.pattern + "+" + queryParameters.pattern + ") and (" + yColumnName + " between " + queryParameters.pattern + "-" + queryParameters.pattern + " and " + queryParameters.pattern + "+" + queryParameters.pattern + ")"
    
def addCircleCondition(queryParameters, xColumnName, yColumnName, queryArgs):
    if queryParameters.hardcode:
        if queryParameters.powermethod:
            return "(power(" + xColumnName + " - " + str(queryParameters.cx) + ",2) + power(" + yColumnName + " - " + str(queryParameters.cy) + ",2) < power(" + str(queryParameters.rad) + ",2))"
        else:
            return "(((" + xColumnName + " - " + str(queryParameters.cx) + ")^2) + ((" + yColumnName + " - " + str(queryParameters.cy) + ")^2) < (" + str(queryParameters.rad) + "^2))"
    else:
        queryArgs.extend([queryParameters.cx,queryParameters.cy,queryParameters.rad])
        if queryParameters.powermethod:
            return "(power(" + xColumnName + " - " + queryParameters.pattern + ",2) + power(" + yColumnName + " - " + queryParameters.pattern + ",2) < power(" + queryParameters.pattern + ",2))"
        else:
            return "(((" + xColumnName + " - " + queryParameters.pattern + ")^2) + ((" + yColumnName + " - " +queryParameters.pattern + ")^2) < (" + queryParameters.pattern + "^2))"

def addOrderByDistance(queryParameters, xColumnName, yColumnName, queryArgs):
    if queryParameters.hardcode:
        if queryParameters.powermethod:
            return " ORDER BY (power(" + xColumnName + " - " + str(queryParameters.cx) + ",2) + power(" + yColumnName + " - " + str(queryParameters.cy) + ",2))"
        else:
            return " ORDER BY (((" + xColumnName + " - " + str(queryParameters.cx) + ")^2) + ((" + yColumnName + " - " + str(queryParameters.cy) + ")^2))"
    else:
        queryArgs.extend([queryParameters.cx, queryParameters.cy])
        if queryParameters.powermethod:
            return " ORDER BY (power(" + xColumnName + " - " + queryParameters.pattern + ",2) + power(" + yColumnName + " - " + queryParameters.pattern + ",2))"
        else:
            return " ORDER BY (((" + xColumnName + " - " +queryParameters.pattern + ")^2) + ((" + yColumnName + " - " + queryParameters.pattern + ")^2))"
    
def addLimit(queryParameters, queryArgs):
    if queryParameters.db == 'ora':
        return " WHERE ROWNUM <= " + str(queryParameters.num)
    else:
        queryArgs.append(queryParameters.num)
        return " LIMIT " + queryParameters.pattern
    
def getWhereStatement(conditions, operator = ' AND '):
    if type(conditions) not in (list, tuple):
        conditions = [conditions,]
    cs = []
    for condition in conditions:
        if condition != '':
            cs.append(condition)
    if len(cs):
        return ' WHERE ' + (operator.join(cs)) + ' '
    return ''

def distinctTable(cursor, tableName, executeMethod):
    tempTable = 'DIRT_' + tableName
    executeMethod(cursor, 'alter table  ' + tableName + ' rename to ' + tempTable)
    executeMethod(cursor, 'CREATE TABLE  ' + tableName + ' AS SELECT DISTINCT * FROM ' + tempTable)
    executeMethod(cursor, 'DROP TABLE  ' + tempTable)
    cursor.connection.commit()

def getSelect(queryParameters, flatTable, addContainsConditionMethod, columnsNameDict, hints = None):
    queryArgs = []
    xname = columnsNameDict['x'][0]
    yname = columnsNameDict['y'][0]
    zname = columnsNameDict['z'][0]
    if queryParameters.queryType in ('rectangle', 'generic'):
        bBoxCondition = addBBoxCondition(queryParameters, xname, yname, queryArgs)
    else:
        bBoxCondition = addBBoxCircleCondition(queryParameters, xname, yname, queryArgs)
    zCondition = addZCondition(queryParameters, zname, queryArgs)
    cols = getSelectCols(queryParameters.columns, columnsNameDict, queryParameters.statistics)
    
    if hints == None:
        hints = ''
    
    if queryParameters.queryType == 'rectangle':
        query = "SELECT " + hints + cols + " FROM " + flatTable + getWhereStatement([bBoxCondition,zCondition])
    elif queryParameters.queryType == 'circle':
        specificCondition = addCircleCondition(queryParameters, xname, yname, queryArgs)
        query = "SELECT "  + cols + " FROM (select " + hints + "* FROM " + flatTable  + getWhereStatement(bBoxCondition,zCondition) + ") b " + getWhereStatement(specificCondition)
    elif queryParameters.queryType == 'generic':
        (specificCondition,queryTable) = addContainsConditionMethod(queryParameters, queryArgs, xname, yname)
        if queryParameters.db != 'ora':
            tables = ['a']
            if queryTable != None:
                tables.append(queryTable)
            query = "SELECT " + cols + " FROM ( SELECT * FROM " + flatTable + getWhereStatement([bBoxCondition,zCondition]) + ") " + ",".join(tables) + getWhereStatement(specificCondition)
        else:
            query = "SELECT " + cols + " from table ( sdo_PointInPolygon ( cursor ( select " + hints + "* FROM " + flatTable + getWhereStatement([bBoxCondition,zCondition]) + " ), " + specificCondition + "))"
    elif queryParameters.queryType == 'nn' :
        orderBy = addOrderByDistance(queryParameters, xname, yname, queryArgs)
        limit = addLimit(queryParameters, queryArgs)
        if queryParameters.db != 'ora':
            query = "SELECT " + cols + " FROM " + flatTable + getWhereStatement([bBoxCondition,zCondition]) + orderBy + limit
        else:
            query = "SELECT "  + cols + " FROM (SELECT " + hints + "* FROM " + flatTable + getWhereStatement([bBoxCondition,zCondition]) + " ) b " + limit + orderBy
    else:
        raise Exception('ERROR: ' + queryParameters.queryType + ' not supported!')
    
    return (query, queryArgs)

def getSelectMorton(iMortonRanges, xMortonRanges, queryParameters, flatTable, addContainsConditionMethod, columnsNameDict, hints = None):
    queryArgs = []
    xname = columnsNameDict['x'][0]
    yname = columnsNameDict['y'][0]
    zname = columnsNameDict['z'][0]
    kname = columnsNameDict['k'][0]
    query = ''
    
    if hints == None:
        hints = '' 

    if len(iMortonRanges):
        if queryParameters.queryType == 'nn':
            raise Exception('If using NN len(iMortonRanges) must be 0!')
        cols = getSelectCols(queryParameters.columns, columnsNameDict)
        inConditions = [
            addMortonCondition(queryParameters, iMortonRanges, kname, queryArgs),
            addZCondition(queryParameters, zname, queryArgs)]
        query = "SELECT " + cols + " FROM " + flatTable + getWhereStatement(inConditions) + " UNION "
    else:
        cols = getSelectCols(queryParameters.columns, columnsNameDict, queryParameters.statistics)
    
    mortonCondition = addMortonCondition(queryParameters, xMortonRanges, kname, queryArgs)
    zCondition = addZCondition(queryParameters, zname, queryArgs)
    
    if queryParameters.queryType in ('rectangle', 'circle'):
        if queryParameters.queryType == 'rectangle' :
            specificCondition = addBBoxCondition(queryParameters, xname, yname, queryArgs)
        else:
            specificCondition = addCircleCondition(queryParameters, xname, yname, queryArgs)
        query += "SELECT " + hints + cols + " FROM (SELECT * FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) + ") a " + getWhereStatement(specificCondition)
    elif queryParameters.queryType == 'generic' :
        (specificCondition,queryTable) = addContainsConditionMethod(queryParameters,queryArgs, xname, yname)
        if queryParameters.db != 'ora':
            tables = ['a']
            if queryTable != None:
                tables.append(queryTable)
            query += "SELECT " + cols + " FROM (SELECT * FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) + ") " + ",".join(tables) + getWhereStatement(specificCondition)
        else:
            query += "SELECT " + hints + cols + " from table ( sdo_PointInPolygon ( cursor ( select " + hints + "* FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) + " ), " + specificCondition + "))"
    elif queryParameters.queryType == 'nn':
        orderBy = addOrderByDistance(queryParameters, xname, yname, queryArgs)
        limit = addLimit(queryParameters, queryArgs)
        if queryParameters.db != 'ora':
            query = "SELECT " + cols + " FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) + orderBy + limit
        else:
            query = "SELECT "  + cols + " FROM (SELECT " + hints + "* FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) + " ) b " + limit + orderBy
    else:
        #Approximation
        query += "SELECT " + hints + cols + " FROM " + flatTable + getWhereStatement([mortonCondition,zCondition]) 
    return (query, queryArgs)
    
def genericQueryParallelGrid(cursor, executeMethod, columns, columnsNameTypeDict, statistics, resultTable, gridTable, createGridTableMethod, childMethod, numProcessesQuery, distinct):
    if distinct and (('x' not in columns) or ('x' not in columns) or ('z' not in columns)):
        raise Exception('GRID distinct requires to have access to columns x, y and z!')    

    (nrows,ncols) = utils.getNRowNCol(numProcessesQuery)
    
    t0 = time.time()
    
    createResultsTable(cursor, executeMethod, resultTable, columns, columnsNameTypeDict, None)
    createGridTableMethod(cursor, gridTable, ncols, nrows)

    children = []
    for i in range(numProcessesQuery):
        children.append(multiprocessing.Process(target=childMethod, args=(i, gridTable)))
        children[-1].start()  
    # wait for all children to finish their execution
    for i in range(numProcessesQuery):
        children[i].join()
        
    if distinct:
        distinctTable(cursor, resultTable, executeMethod)
        
    return getResult(cursor, t0, resultTable, columnsNameTypeDict, False, columns, statistics)

def genericQueryParallelCand(cursor, executeMethod, columns, columnsNameTypeDict, statistics, resultTable, idsQuery, idsQueryArgs, childMethod, numProcessesQuery):
    t0 = time.time()
    
    createResultsTable(cursor, executeMethod, resultTable, columns, columnsNameTypeDict, None)
    
    executeMethod(cursor, idsQuery, idsQueryArgs)
    
    blkIds = numpy.array(cursor.fetchall())[:,0]
    children = []
    for chunkIds in numpy.array_split(blkIds, numProcessesQuery):
        children.append(multiprocessing.Process(target=childMethod, 
            args=(chunkIds,)))
        children[-1].start()  
    # wait for all children to finish their execution
    for i in range(numProcessesQuery):
        children[i].join()
        
    return getResult(cursor, t0, resultTable, columnsNameTypeDict, False, columns, statistics)

def parallelMorton(iMortonRanges, xMortonRanges, childMethod, numProcessesQuery):
    if iMortonRanges != None:
        numMRanges = max((len(iMortonRanges), len(xMortonRanges)))
        if numMRanges > numProcessesQuery:
            numChunks = numProcessesQuery
        else:
            numChunks = numMRanges
        ichunks = numpy.array_split(iMortonRanges, numChunks)
        xchunks = numpy.array_split(xMortonRanges, numChunks)
    else:
        numMRanges = len(xMortonRanges)
        if numMRanges > numProcessesQuery:
            numChunks = numProcessesQuery
        else:
            numChunks = numMRanges
        ichunks = numpy.array_split([], numChunks)
        xchunks = numpy.array_split(xMortonRanges, numChunks)
    children = []
    for i in range(numChunks):
        children.append(multiprocessing.Process(target=childMethod, 
            args=(ichunks[i],xchunks[i])))
        children[-1].start()  
    # wait for all children to finish their execution
    for i in range(numChunks):
        children[i].join()
