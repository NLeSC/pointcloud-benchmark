#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, math, copy, subprocess
from pointcloud import dbops
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from itertools import groupby, count

class Querier(AbstractQuerier):   
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        
        self.columnsNameDict = {'x':["PC_Get(qpoint, 'x')"],
                                'y':["PC_Get(qpoint, 'y')"],
                                'z':["PC_Get(qpoint, 'z')"]
                                }
        
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cursor()
    
        self.prepareQuery(queryId, queriesParameters, cursor, False)
        gridTable = 'query_grid_' + str(self.queryIndex)
        
        for table in (self.resultTable, gridTable):
            self.dropTable(cursor, table, True) 
    
        if self.numProcessesQuery == 1:
            t0 = time.time()
            (query, queryArgs) = self.getSelect(self.qp)
            self.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS ( " + query + " )", queryArgs)
            connection.commit()
                              
            (eTime, result) =  dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics)
        else:
            if self.qp.queryType in ('rectangle','circle','generic'):       
                if self.parallelType == 'cand':
                    idsQuery = "SELECT " + self.blockTable +".id FROM " + self.blockTable +", (SELECT ST_GeomFromEWKT(%s) as geom) A WHERE pc_intersects(pa,geom)"
                    idsQueryArgs =  ['SRID='+self.srid+';'+self.wkt, ]
                    (eTime, result) = dbops.genericQueryParallelCand(cursor,self.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, idsQuery, idsQueryArgs, 
                                                                     self.runGenericQueryParallelCandChild, self.numProcessesQuery)
                else:     
                    (eTime, result) = dbops.genericQueryParallelGrid(cursor, self.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                                     self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                                     (self.parallelType == 'griddis'))
        connection.close()
        return (eTime, result)

    def queryMulti(self, queryId, iterationId, queriesParameters):
        self.prepareQuery(queryId, queriesParameters, None, None)    
        connection = self.connect()
        cursor = connection.cursor()
        t0 = time.time()
        (query, queryArgs) = self.getSelect(self.qp)
        sqlFileName = str(queryId) + '.sql'
        sqlFile = open(sqlFileName, 'w')
        sqlFile.write(cursor.mogrify(query, queryArgs) + ';\n')
        sqlFile.close()
        command = 'psql ' + self.connectString(False, True) + ' < ' + sqlFileName + ' | wc -l'
        result = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].replace('\n','')
        eTime = time.time() - t0
        try:
            result  = int(result) - 4
        except:
            result = -1
        return (eTime, result)


    def getSelect(self, qp):
        cols = dbops.getSelectCols(qp.columns, self.columnsNameDict, qp.statistics, True)
        if qp.queryType != 'nn':
            queryArgs = ['SRID='+self.srid+';'+self.wkt, ]
            zCondition = dbops.addZCondition(qp, self.columnsNameDict['z'][0], queryArgs)   
            query = "SELECT " + cols + " from (SELECT pc_explode(pc_intersection(pa,geom)) AS qpoint from " + self.blockTable + ", (SELECT ST_GeomFromEWKT(%s) as geom) A WHERE pc_intersects(pa,geom)) AS qtable " + dbops.getWhereStatement(zCondition)
        else:
            numBlocksNeigh = int(math.pow(2 + math.ceil(math.sqrt(math.ceil(float(qp.num)/float(self.blockSize)))), 2))
            queryArgs = ['SRID='+self.srid+';'+self.wkt, numBlocksNeigh]            
            zCondition = dbops.addZCondition(qp, self.columnsNameDict['z'][0], queryArgs)
            queryArgs.extend([qp.cx, qp.cy, qp.num])
            orderBy = "ORDER BY ((" + self.columnsNameDict['x'][0] + " - %s)^2 + (" + self.columnsNameDict['y'][0] + " - %s)^2)"
            query = "SELECT " + cols + " FROM ( SELECT PC_explode(pa) as qpoint FROM  (SELECT pa FROM " + self.blockTable + " ORDER BY geometry(pa) <#> %s LIMIT ST_GeomFromEWKT(%s)) as A ) as B " + dbops.getWhereStatement(zCondition) + orderBy + " LIMIT %s"
        return (query,queryArgs)


    def addContains(self, queryArgs, queryIndex, queryTable):
        queryArgs.append(queryIndex)
        return "pc_intersects(pa,geom) and " + queryTable + ".id = %s"
    
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
        connection = self.connect()
        cursor = connection.cursor() 
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = self.getSelectParallel(cursor, cqp, gridTable, sIndex+1)
        self.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.commit()
        connection.close()
     
    def runGenericQueryParallelCandChild(self, chunkIds):
        connection = self.connect()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = self.getSelectParallel(cursor, cqp, self.queryTable, self.queryIndex, True, chunkIds)
        self.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.commit()
        connection.close()      
