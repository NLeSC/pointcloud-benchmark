#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, math, copy
from pointcloud import dbops, postgresops
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from itertools import groupby, count

class Querier(AbstractQuerier):          
    def initialize(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        cursor.execute('SELECT srid from pointcloud_formats LIMIT 1')
        self.srid = cursor.fetchone()[0]
        
        postgresops.dropTable(cursor, self.queryTable, check = True)
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + str(self.srid) + "));")
        
        connection.close()
        
        self.columnsNameDict = {'x':["PC_Get(qpoint, 'x')"],
                                'y':["PC_Get(qpoint, 'y')"],
                                'z':["PC_Get(qpoint, 'z')"]}
                
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor()
    
        self.prepareQuery(cursor, queryId, queriesParameters, iterationId == 0)
        postgresops.dropTable(cursor, self.resultTable, True)
        
        if self.qp.queryMethod != 'stream' and self.numProcessesQuery > 1 and self.qp.queryType in ('rectangle','circle','generic') :
             return self.pythonParallelization()
        
        t0 = time.time()
        (query, queryArgs) = self.getSelect(self.qp)
        
        if self.qp.queryMethod != 'stream': # disk or stat
            postgresops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS ( " + query + " )", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, None, True, self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            postgresops.createSQLFile(cursor, sqlFileName, query, queryArgs)
            
            result = postgresops.executeSQLFileCount(self.getConnectionString(False, True), sqlFileName)
            eTime = time.time() - t0
        connection.close()
        return (eTime, result)
   
    def getSelect(self, qp):
        cols = dbops.getSelectCols(qp.columns, self.columnsNameDict, qp.statistics, True)
        if qp.queryType != 'nn':
            queryArgs = [self.queryIndex, ]
            zCondition = dbops.addZCondition(qp, self.columnsNameDict['z'][0], queryArgs)   
            query = "SELECT " + cols + " from (SELECT pc_explode(pc_intersection(pa,geom)) AS qpoint from " + self.blockTable + ", (SELECT geom FROM " + self.queryTable + " WHERE id = %s) A WHERE pc_intersects(pa,geom)) AS qtable " + dbops.getWhereStatement(zCondition)
        else:
            numBlocksNeigh = int(math.pow(2 + math.ceil(math.sqrt(math.ceil(float(qp.num)/float(self.blockSize)))), 2))
            queryArgs = [self.queryIndex, numBlocksNeigh]            
            zCondition = dbops.addZCondition(qp, self.columnsNameDict['z'][0], queryArgs)
            queryArgs.extend([qp.cx, qp.cy, qp.num])
            orderBy = "ORDER BY ((" + self.columnsNameDict['x'][0] + " - %s)^2 + (" + self.columnsNameDict['y'][0] + " - %s)^2)"
            query = "SELECT " + cols + " FROM ( SELECT PC_explode(pa) as qpoint FROM  (SELECT pa FROM " + self.blockTable + ", " + self.queryTable + " C WHERE C.id = %s ORDER BY geometry(pa) <#> geom LIMIT %s) as A ) as B " + dbops.getWhereStatement(zCondition) + orderBy + " LIMIT %s"
        return (query,queryArgs)

    
    #
    # METHOD RELATED TO THE QUERIES OUT-OF-CORE PYTHON PARALLELIZATION 
    #
    def pythonParallelization(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        if self.parallelType == 'cand':
            idsQuery = "SELECT " + self.blockTable +".id FROM " + self.blockTable +", (SELECT geom FROM " + self.queryTable + " WHERE id = %s) A WHERE pc_intersects(pa,geom)"
            idsQueryArgs =  [self.queryIndex, ]
            (eTime, result) = dbops.genericQueryParallelCand(cursor,postgresops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                             self.qp.statistics, self.resultTable, idsQuery, idsQueryArgs, 
                                                             self.runGenericQueryParallelCandChild, self.numProcessesQuery)
        else:     
            gridTable = 'query_grid_' + str(self.queryIndex)
            postgresops.dropTable(cursor, gridTable, True)
            (eTime, result) = dbops.genericQueryParallelGrid(cursor, postgresops.mogrifyExecute, self.qp.columns, self.colsData, 
                                                             self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                             self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                             (self.parallelType == 'griddis'))
        connection.close()
        return (eTime, result)
    
    def addContainsChunkIds(self, queryArgs, queryIndex, queryTable, chunkIds):
        queryArgs.append(queryIndex)
        elements = []
        for _,crange in groupby(chunkIds, lambda n, c=count(): n-next(c)):
            listcrange = list(crange)
            if len(listcrange) == 1:
                queryArgs.append(listcrange[0])
                elements.append('(' + self.blockTable + '.id=%s)')
            else:
                elements.append('(' + self.blockTable + '.id between %s and %s)')
                queryArgs.append(listcrange[0])
                queryArgs.append(listcrange[-1])
        return queryTable + ".id = %s AND (" + ' OR '.join(elements) + ")"

    def addContains(self, queryArgs, queryIndex, queryTable):
        queryArgs.append(queryIndex)
        return "pc_intersects(pa,geom) and " + queryTable + ".id = %s" 

    def getSelectParallel(self, cursor, qp, queryTable, queryIndex, isCand = False, chunkIds = None):
        cols = dbops.getSelectCols(qp.columns, self.columnsNameDict, qp.statistics, True)
        queryArgs =  []
        if isCand:
            specCondition = self.addContainsChunkIds(queryArgs, queryIndex, queryTable, chunkIds) 
        else:
            specCondition = self.addContains(queryArgs, queryIndex, queryTable)

        zCondition = dbops.addZCondition(qp, self.columnsNameDict['z'][0], queryArgs)    
        query = "SELECT " + cols + " from (SELECT pc_explode(pc_intersection(pa,geom)) AS qpoint from " + self.blockTable + ", " + queryTable + dbops.getWhereStatement(specCondition) + ") AS qtable "+ dbops.getWhereStatement(zCondition)
        return (query,queryArgs)
            
    def runGenericQueryParallelGridChild(self, sIndex, gridTable):
        connection = self.getConnection()
        cursor = connection.cursor() 
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = self.getSelectParallel(cursor, cqp, gridTable, sIndex+1)
        postgresops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.close()
     
    def runGenericQueryParallelCandChild(self, chunkIds):
        connection = self.getConnection()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = self.getSelectParallel(cursor, cqp, self.queryTable, self.queryIndex, True, chunkIds)
        postgresops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.close()      
