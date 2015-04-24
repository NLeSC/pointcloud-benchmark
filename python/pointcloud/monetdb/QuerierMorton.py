#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from shapely.wkt import loads, dumps
import time,logging
from pointcloud.monetdb.AbstractQuerier import AbstractQuerier
from pointcloud import wktops, dbops, monetdbops
from pointcloud.QuadTree import QuadTree

MAXIMUM_RANGES = 10

class QuerierMorton(AbstractQuerier):      
    def initialize(self):
        #Variables used during query
        self.queryIndex = None
        self.resultTable = None
        self.qp = None
        connection = self.getConnection()
        cursor = connection.cursor()
        logging.info('Getting SRID and extent from ' + self.dbName)
        monetdbops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = cursor.fetchone()

        # Create the quadtree
        qtDomain = (0, 0, int((self.maxX-self.minX)/self.scaleX), int((self.maxY-self.minY)/self.scaleY))
        self.quadtree = QuadTree(qtDomain, 'auto')    
        # Differentiate QuadTree nodes that are fully in the query region
        self.mortonDistinctIn = False
        
        self.queryColsData = self.DM_FLAT.copy()
        if not ('x' in self.columns and 'y' in self.columns):
            self.queryColsData['x'][0] = 'GetX(morton2D, ' + str(self.scaleX) + ', ' + str(int(self.minX / self.scaleX)) + ')' 
            self.queryColsData['y'][0] = 'GetY(morton2D, ' + str(self.scaleY) + ', ' + str(int(self.minY / self.scaleY)) + ')'
            
        # Drops possible query table 
        monetdbops.dropTable(cursor, utils.QUERY_TABLE, check = True)
        # Create query table
        cursor.execute("CREATE TABLE " +  utils.QUERY_TABLE + " (id integer, geom Geometry);")
        connection.commit()
                    
        connection.close()
    
    def query(self, queryId, iterationId, queriesParameters):
        (eTime, result) = (-1, None)
        connection = self.getConnection()
        cursor = connection.cursor() 
        self.prepareQuery(cursor, queryId, queriesParameters, iterationId == 0)
        monetdbops.dropTable(cursor, self.resultTable, True)    
           
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
        
        (query, queryArgs) = dbops.getSelectMorton(mimranges, mxmranges, self.qp, self.flatTable, self.addContainsCondition, self.queryColsData)
        
        if self.qp.queryMethod != 'stream': # disk or stat
            monetdbops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS " + query + " WITH DATA", queryArgs)
            (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.DM_FLAT, True, self.qp.columns, self.qp.statistics)
        else:
            sqlFileName = str(queryId) + '.sql'
            monetdbops.createSQLFile(sqlFileName, query, queryArgs)
            result = monetdbops.executeSQLFileCount(self.dbName, sqlFileName)
            eTime = time.time() - t0
        connection.close()
        return (eTime, result)