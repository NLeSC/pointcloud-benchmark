#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time,copy,logging
from pointcloud.oracle.AbstractQuerier import AbstractQuerier
from pointcloud import wktops, dbops, qtops, oracleops

class QuerierMorton(AbstractQuerier):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        # Create the quadtree
        connection = self.getConnection()
        cursor = connection.cursor()
        
        oracleops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, minX, minY, maxX, maxY, scaleX, scaleY) = cursor.fetchone()[0]
        
        (self.quadtree, self.mortonDistinctIn, self.mortonApprox, self.maxRanges) = qtops.getQuadTree(configuration, minX, minY,maxX,maxY,scaleX,scaleY)

    def queryDisk(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.prepareQuery(queryId, queriesParameters, iterationId == 0)
    
        if self.mortonApprox:
            self.queryType = 'approx'
        
        oracleops.dropTable(cursor, self.resultTable, True)    
       
        t0 = time.time()
        
        (mimranges,mxmranges) = self.quadtree.getMortonRanges(wktops.scale(self.qp.wkt, float(self.mortonScaleX), float(self.mortonScaleY)), self.mortonDistinctIn, maxRanges = self.maxRanges )
        if len(mimranges) == 0 and len(mxmranges) == 0:
            logging.info('None morton range in specified extent!')
            return

        self.hints = []
        if not self.cluster and self.index != 'false': 
            self.hints.append('INDEX(' + self.flatTable + ' ' + self.flatTable + '_IDX)')
            
        if self.parallelType != 'nati':
            connection.commit()
            dbops.createResultsTable(cursor, oracleops.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData)
            dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)
        else:
            if self.numProcessesQuery > 1:
                self.hints.append('PARALLEL (' + str(self.numProcessesQuery) + ')')
            (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.colsData, self.getHintStatement(self.hints))
            oracleops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + "", queryArgs)
            connection.commit()
        
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn) and (self.parallelType == 'nati'), self.qp.columns, self.qp.statistics)
        connection.close()
        return (eTime, result)
            
    def getHintStatement(self, hints):
        if len(hints):
            return '/*+ ' + ' '.join(hints) + ' */'
        return ''
    
    def childInsert(self, iMortonRanges, xMortonRanges):
        connection = self.getConnection()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = dbops.getSelectMorton(iMortonRanges, xMortonRanges, cqp, self.flatTable, self.addContainsCondition, self.colsData, self.getHintStatement(self.hints))
        oracleops.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.commit()
        connection.close()
