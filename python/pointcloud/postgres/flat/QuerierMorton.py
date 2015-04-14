#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from shapely.wkt import loads, dumps
import time,copy,logging 
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud import qtops,wktops,dbops,postgresops

MAXIMUM_RANGES = 10

class QuerierMorton(AbstractQuerier):        
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        
        connection = self.getConnection()
        cursor = connection.cursor()
        postgresops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = cursor.fetchone()[0]
        connection.close()
        
        qtDomain = (0, 0, int((self.maxX-self.minX)/self.scaleX), int((self.maxY-self.minY)/self.scaleY))
        self.quadtree = QuadTree(qtDomain, 'auto')    
        # Differentiate QuadTree nodes that are fully in the query region
        self.mortonDistinctIn = False
    
    
    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        queryArgs.extend(['SRID='+self.srid+';'+self.qp.wkt, self.srid, self.srid ])
        return (" _ST_Contains(geom, st_setSRID(st_makepoint(" + xname + "," + yname + "),%s))", '(SELECT ST_GeomFromEWKT(%s) as geom) A')

    def queryDisk(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.prepareQuery(queryId, queriesParameters, cursor, iterationId == 0)
        
        postgresops.dropTable(cursor, self.resultTable, True)    
        
        wkt = self.qp.wkt
        if self.qp.queryType == 'nn':
            g = loads(self.qp.wkt)
            wkt = dumps(g.buffer(self.qp.rad))
       
        t0 = time.time()
        scaledWKT = wktops.scale(wkt, float(self.mortonScaleX), float(self.mortonScaleY), float(self.mortonGlobalOffsetX), float(self.mortonGlobalOffsetY))    
        (mimranges,mxmranges) = self.quadtree.getMortonRanges(scaledWKT, self.mortonDistinctIn, maxRanges = MAXIMUM_RANGES)

        if len(mimranges) == 0 and len(mxmranges) == 0:
            logging.info('None morton range in specified extent!')
            return

        if self.numProcessesQuery == 1:
            (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.colsData)
            postgresops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS (" + query + ")", queryArgs)
        else:
            dbops.createResultsTable(cursor, postgresops.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData, None)
            dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)  
        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn) and (self.numProcessesQuery == 1), self.qp.columns, self.qp.statistics)
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
