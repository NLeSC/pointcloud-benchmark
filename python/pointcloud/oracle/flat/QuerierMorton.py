#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time,copy,logging
from pointcloud.oracle.AbstractQuerier import AbstractQuerier
from pointcloud import wktops, dbops, oracleops
from pointcloud.QuadTree import QuadTree

MAXIMUM_RANGES = 10

class QuerierMorton(AbstractQuerier):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        
        oracleops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, minX, minY, maxX, maxY, scaleX, scaleY) = cursor.fetchone()[0]
        
        # Create the quadtree
        qtDomain = (0, 0, int((maxX-minX)/scaleX), int((maxY-minY)/scaleY))
        self.quadtree = QuadTree(qtDomain, 'auto')    
        # Differentiate QuadTree nodes that are fully in the query region
        self.mortonDistinctIn = False
        
        connection.close()
        
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor()
        self.prepareQuery(queryId, queriesParameters, iterationId == 0)
        oracleops.dropTable(cursor, self.resultTable, True)    
       
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

        self.hints = []
        if not self.flatTableIOT: 
            self.hints.append('INDEX(' + self.flatTable + ' ' + self.flatTable + '_IDX)')
        
        if self.qp.queryMethod != 'stream' and self.numProcessesQuery > 1 and self.parallelType != 'nati' and self.qp.queryType in ('rectangle','circle','generic') :
            return self.pythonParallelization(t0, mimranges, mxmranges)
        
        if self.numProcessesQuery > 1:
            self.hints.append('PARALLEL (' + str(self.numProcessesQuery) + ')')
        (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.colsData, self.getHintStatement(self.hints))
        
        if self.qp.queryMethod != 'stream': # disk or stat
            oracleops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + "", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn), self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            oracleops.createSQLFile(cursor, sqlFileName, query, queryArgs)
            result = oracleops.executeSQLFileCount(self.getConnectionString(False), sqlFileName)
            eTime = time.time() - t0
            
        connection.close()
        return (eTime, result)
            
    def getHintStatement(self, hints):
        if len(hints):
            return '/*+ ' + ' '.join(hints) + ' */'
        return ''
    
    #
    # METHOD RELATED TO THE QUERIES OUT-OF-CORE PYTHON PARALLELIZATION 
    #
    def pythonParallelization(self, t0, mimranges, mxmranges):
        connection = self.getConnection()
        cursor = connection.cursor()
        dbops.createResultsTable(cursor, oracleops.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData)
        dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, False, self.qp.columns, self.qp.statistics)
        connection.close()
        return (eTime, result)

    def childInsert(self, iMortonRanges, xMortonRanges):
        connection = self.getConnection()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = dbops.getSelectMorton(iMortonRanges, xMortonRanges, cqp, self.flatTable, self.addContainsCondition, self.colsData, self.getHintStatement(self.hints))
        oracleops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.close()
