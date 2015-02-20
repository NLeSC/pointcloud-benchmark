#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from shapely.wkt import loads, dumps
import time,logging
from pointcloud.monetdb.AbstractQuerier import AbstractQuerier
from pointcloud import wktops, dbops, qtops

class QuerierMorton(AbstractQuerier):        
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        
        # Create the quadtree
        (self.quadtree, self.mortonDistinctIn, self.mortonApprox, self.maxRanges) = qtops.getQuadTree(configuration, float(self.minX), float(self.minY), float(self.maxX), float(self.maxY), float(self.mortonScaleX), float(self.mortonScaleY), float(self.mortonGlobalOffsetX), float(self.mortonGlobalOffsetY))
    
    def query(self, queryId, iterationId, queriesParameters):
        connection = self.connect()
        cursor = connection.cursor()
        
        self.prepareQuery(queryId, queriesParameters)
            
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
        if not ('x' in self.columns and 'y' in self.columns):
            colsData = self.colsData.copy()
            colsData['x'][0] = 'GetX(morton2D, ' + str(self.mortonScaleX) + ', ' + str(int(float(self.mortonGlobalOffsetX) / float(self.mortonScaleX))) + ')' 
            colsData['y'][0] = 'GetY(morton2D, ' + str(self.mortonScaleY) + ', ' + str(int(float(self.mortonGlobalOffsetY) / float(self.mortonScaleY))) + ')'
        else:
            colsData = self.colsData

        (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, colsData)
        self.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + " WITH DATA", queryArgs)
        
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, not self.mortonDistinctIn, self.qp.columns, self.qp.statistics)

        connection.close()
        return (eTime, result)