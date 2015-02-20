#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time
from pointcloud import dbops
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud.QueryParameters import QueryParameters

class Querier(AbstractQuerier):        
    """ Querier for tables with X,Y,Z (and possibly B-Tree index, but not required)"""
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cusor()

        self.prepareQuery(queryId, queriesParameters, cursor, iterationId == 0)
        gridTable = 'query_grid_' + str(self.queryIndex)
        
        for table in (self.resultTable, gridTable):
            self.dropTable(cursor, table, True) 
    
        if self.numProcessesQuery > 1:
            if self.qp.queryType in ('rectangle','circle','generic'):            
                (eTime, result) = dbops.genericQueryParallelGrid(cursor, self.mogrifyExecute, self.qp.columns, self.colsData, 
                                                                     self.qp.statistics, self.resultTable, gridTable, self.createGridTableMethod,
                                                                     self.runGenericQueryParallelGridChild, self.numProcessesQuery, 
                                                                     (self.parallelType == 'griddis'))
        else:
            t0 = time.time()
            (query, queryArgs) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.colsData)
            self.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS ( " + query + " )", queryArgs)
            connection.commit()
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, True, self.qp.columns, self.qp.statistics)
        connection.close()
        return (eTime, result)
        
    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        queryArgs.extend(['SRID='+self.srid+';'+self.wkt, self.srid, ])
        return (" _ST_Contains(geom, st_setSRID(st_makepoint(" + xname + "," + yname + "),%s))", '(SELECT ST_GeomFromEWKT(%s) as geom) A')
    
    def runGenericQueryParallelGridChild(self, sIndex, gridTable):
        connection = self.connect()
        cursor = connection.cursor()
        (minx,maxx,miny,maxy) = self.getBBoxGeometry(cursor, gridTable, sIndex+1)
        
        qType = 'generic'
        if self.qp.queryType == 'rectangle':
            qType = 'rectangle'
        
        self.queryIndex = sIndex+1
        self.queryTable = gridTable
           
        cqp = QueryParameters('psql',None,qType,self.qp.columns,None,minx,maxx,miny,maxy,None,None,None,self.qp.minz,self.qp.maxz,None,None,None,None)
        (query, queryArgs) = dbops.getSelect(cqp, self.flatTable, self.addContainsCondition, self.colsData)
        self.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.commit()
        connection.close()
