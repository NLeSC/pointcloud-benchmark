#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time
from pointcloud import dbops, postgresops
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud.QueryParameters import QueryParameters

class Querier(AbstractQuerier):        
    """ Querier for tables with X,Y,Z (and possibly B-Tree index, but not required)"""
    def initialize(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.metaTable = self.blockTable + '_meta'
        postgresops.mogrifyExecute(cursor, "SELECT srid from " + self.metaTable)
        self.srid = cursor.fetchone()[0]

        postgresops.dropTable(cursor, self.queryTable, check = True)
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + str(self.srid) + "));")
        connection.close()
        
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor()
    
        self.prepareQuery(cursor, queryId, queriesParameters, iterationId == 0)
        postgresops.dropTable(cursor, self.resultTable, True) 
        
        if self.numProcessesQuery > 1:
            if self.qp.queryMethod != 'stream' and self.qp.queryType in ('rectangle','circle','generic') :
                 return self.pythonParallelization()
            else:
                 logging.error('Python parallelization only available for disk queries (CTAS) which are not NN queries!')
                 return (eTime, result)
        
        t0 = time.time()
        (query, queryArgs) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.colsData)
        
        if self.qp.queryMethod != 'stream': # disk or stat
            postgresops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS ( " + query + " )", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            postgresops.createSQLFile(cursor, sqlFileName, query, None)
            result = postgresops.executeSQLFileCount(self.getConnectionString(False, True), sqlFileName)
            eTime = time.time() - t0
        connection.close()
        return (eTime, result)

    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        queryArgs.extend([self.queryIndex, self.srid, ])
        return (self.queryTable, " id = %s AND _ST_Contains(geom, st_setSRID(st_makepoint(" + xname + "," + yname + "),%s))")
    
            
    #
    # METHOD RELATED TO THE QUERIES OUT-OF-CORE PYTHON PARALLELIZATION 
    #
    def pythonParallelization(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        gridTable = ('query_grid_' + str(self.queryIndex)).upper()
        postgresops.dropTable(cursor, gridTable, True)
        (eTime, result) =  dbops.genericQueryParallelGrid(cursor, postgresops.mogrifyExecute, self.qp.columns, self.colsData, 
             self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
             self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
             (self.parallelType == 'griddis'))
        connection.close()
        return (eTime, result)
    
    def runGenericQueryParallelGridChild(self, sIndex, gridTable):
        connection = self.getConnection()
        cursor = connection.cursor()
        (minx,maxx,miny,maxy) = self.getBBoxGeometry(cursor, gridTable, sIndex+1)
        
        qType = 'generic'
        if self.qp.queryType == 'rectangle':
            qType = 'rectangle'
        
        self.queryIndex = sIndex+1
        self.queryTable = gridTable
           
        cqp = QueryParameters('psql',None,qType,self.qp.columns,None,minx,maxx,miny,maxy,None,None,None,self.qp.minz,self.qp.maxz,None,None,None,None)
        (query, queryArgs) = dbops.getSelect(cqp, self.flatTable, self.addContainsCondition, self.colsData)
        postgresops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.close()
