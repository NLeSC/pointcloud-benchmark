#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from shapely.wkt import loads, dumps
import time,copy,logging 
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud import qtops,wktops,dbops

class QuerierMorton(AbstractQuerier):        
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        # Create the quadtree
        (self.quadtree, self.mortonDistinctIn, self.mortonApprox, self.maxRanges) = qtops.getQuadTree(configuration, float(self.minX), float(self.minY), float(self.maxX), float(self.maxY), float(self.mortonScaleX), float(self.mortonScaleY), float(self.mortonGlobalOffsetX), float(self.mortonGlobalOffsetY))
    
    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        queryArgs.extend(['SRID='+self.srid+';'+self.wkt, self.srid, self.srid ])
        return (" _ST_Contains(geom, st_setSRID(st_makepoint(" + xname + "," + yname + "),%s))", '(SELECT ST_GeomFromEWKT(%s) as geom) A')

    def query(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cursor()
        
        self.prepareQuery(queryId, queriesParameters, cursor, iterationId == 0)
        
        if self.mortonApprox:
            self.queryType = 'approx'
        
        self.dropTable(cursor, self.resultTable, True)    
        
        wkt = self.wkt
        if self.qp.queryType == 'nn':
            g = loads(self.wkt)
            wkt = dumps(g.buffer(self.qp.rad))
       
        t0 = time.time()
        scaledWKT = wktops.scale(wkt, float(self.mortonScaleX), float(self.mortonScaleY), float(self.mortonGlobalOffsetX), float(self.mortonGlobalOffsetY))    
        (mimranges,mxmranges) = self.quadtree.getMortonRanges(scaledWKT, self.mortonDistinctIn, maxRanges = self.maxRanges)

        if len(mimranges) == 0 and len(mxmranges) == 0:
            logging.info('None morton range in specified extent!')
            return

        if self.numProcessesQuery == 1:
            (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.colsData)
            self.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS (" + query + ")", queryArgs)
            connection.commit()
        else:
            dbops.createResultsTable(cursor, self.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData, None)
            connection.commit()
            dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)  
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn) and (self.numProcessesQuery == 1), self.qp.columns, self.qp.statistics)
        connection.close()
        return (eTime, result)
            
    def childInsert(self, iMortonRanges, xMortonRanges):
        connection = self.connect()
        cursor = connection.cursor()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = dbops.getSelectMorton(iMortonRanges, xMortonRanges, cqp, self.flatTable, self.addContainsCondition, self.colsData)
        self.mogrifyExecute(cursor, "INSERT INTO "  + self.resultTable + " " + query, queryArgs)
        connection.commit()
        connection.close()
