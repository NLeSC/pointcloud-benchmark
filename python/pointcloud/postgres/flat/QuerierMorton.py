#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from shapely.wkt import loads, dumps
import time,copy,logging 
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud import wktops,dbops,postgresops
from pointcloud.QuadTree import QuadTree


MAXIMUM_RANGES = 10

class QuerierMorton(AbstractQuerier):        
    def initialize(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.metaTable = self.blockTable + '_meta'
        postgresops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = cursor.fetchone()
        
        postgresops.dropTable(cursor, self.queryTable, check = True)
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + str(self.srid) + "));")
        
        connection.close()
        
        qtDomain = (0, 0, int((self.maxX-self.minX)/self.scaleX), int((self.maxY-self.minY)/self.scaleY))
        self.quadtree = QuadTree(qtDomain, 'auto')    
        # Differentiate QuadTree nodes that are fully in the query region
        self.mortonDistinctIn = False
    
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor()
        self.prepareQuery(cursor, queryId, queriesParameters, iterationId == 0)
        postgresops.dropTable(cursor, self.resultTable, True)    
       
        wkt = self.qp.wkt
        if self.qp.queryType == 'nn':
            g = loads(self.qp.wkt)
            wkt = dumps(g.buffer(self.qp.rad))
       
        t0 = time.time()
        scaledWKT = wktops.scale(wkt, self.scaleX, self.scaleY, self.minX, self.minY)    
        (mimranges,mxmranges) = self.quadtree.getMortonRanges(scaledWKT, self.mortonDistinctIn, maxRanges = MAXIMUM_RANGES)
       
        if len(mimranges) == 0 and len(mxmranges) == 0:
            logging.info('None morton range in specified extent!')
            return (eTime, result)

        if self.qp.queryMethod != 'stream' and self.numProcessesQuery > 1 and self.qp.queryType in ('rectangle','circle','generic') :
            return self.pythonParallelization(t0, mimranges, mxmranges)
        
        (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.colsData, self.getHintStatement(self.hints))
        
        if self.qp.queryMethod != 'stream': # disk or stat
            postgresops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + "", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn), self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            postgresops.createSQLFile(cursor, sqlFileName, query, queryArgs)
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
    def pythonParallelization(self, t0, mimranges, mxmranges):
        connection = self.getConnection()
        cursor = connection.cursor()
        dbops.createResultsTable(cursor, postgresops.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData)
        dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, False, self.qp.columns, self.qp.statistics)
        connection.close()
        return (eTime, result)

    def childInsert(self, iMortonRanges, xMortonRanges):
        connection = self.getConnection()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = dbops.getSelectMorton(iMortonRanges, xMortonRanges, cqp, self.flatTable, self.addContainsCondition, self.colsData)
        postgresops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.close()
