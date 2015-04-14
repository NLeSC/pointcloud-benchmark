#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time,copy,logging
from pointcloud.postgres.AbstractQuerier import AbstractQuerier
from pointcloud import wktops, dbops, postgresops
from pointcloud.QuadTree import QuadTree

MAXIMUM_RANGES = 10

class QuerierMorton(AbstractQuerier):        
    def initialize(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.metaTable = self.blockTable + '_meta'
        postgresops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = cursor.fetchone()[0]

        postgresops.dropTable(cursor, self.queryTable, check = True)
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + self.srid + "));")
        
        connection.close()
        
        self.columnsNameDict = {'x':["PC_Get(qpoint, 'x')"],
                                'y':["PC_Get(qpoint, 'y')"],
                                'z':["PC_Get(qpoint, 'z')"]}
        
        qtDomain = (0, 0, int((self.maxX-self.minX)/self.scaleX), int((self.maxY-self.minY)/self.scaleY))
        self.quadtree = QuadTree(qtDomain, 'auto')    
        # Differentiate QuadTree nodes that are fully in the query region
        self.mortonDistinctIn = False
        
        
    def queryDisk(self, queryId, iterationId, queriesParameters):
        connection = self.getConnection()
        cursor = connection.cursor()
        
        self.prepareQuery(queryId, queriesParameters, cursor, iterationId == 0)
        
        postgresops.dropTable(cursor, self.resultTable, True)    
 
        if self.qp.queryType == 'nn':
            raise Exception('Not support for NN queries!')
       
        t0 = time.time()
        
        (mimranges,mxmranges) = self.quadtree.getMortonRanges(wktops.scale(self.qp.wkt, float(self.mortonScaleX), float(self.mortonScaleY)), self.mortonDistinctIn, maxRanges = MAXIMUM_RANGES)
        if len(mimranges) == 0 and len(mxmranges) == 0:
            logging.info('None morton range in specified extent!')
            return
 
        if self.numProcessesQuery == 1:
            (query, queryArgs) = self.getSelect(self.qp, mimranges, mxmranges)        
            postgresops.mogrifyExecute(cursor, "CREATE TABLE "  + self.resultTable + " AS (" + query + ")", queryArgs)
        else:
            dbops.createResultsTable(cursor, postgresops.mogrifyExecute, self.resultTable, self.qp.columns, self.colsData, None)
            dbops.parallelMorton(mimranges, mxmranges, self.childInsert, self.numProcessesQuery)    

        (eTime, result) = dbops.getResult(cursor, t0, self.resultTable, self.colsData, (not self.mortonDistinctIn) and (self.numProcessesQuery == 1), self.qp.columns, self.qp.statistics)

        connection.close()
        return (eTime, result)
        
    def childInsert(self, iMortonRanges, xMortonRanges):
        connection = self.getConnection()
        cqp = copy.copy(self.qp)
        cqp.statistics = None
        (query, queryArgs) = self.getSelect(cqp, iMortonRanges, xMortonRanges)
        postgresops.mogrifyExecute(connection.cursor(), "INSERT INTO " + self.resultTable + " "  + query, queryArgs)
        connection.close()  
        
    def addContains(self, queryArgs):
        queryArgs.append(self.queryIndex)
        return "pc_intersects(pa,geom) and " + self.queryTable + ".id = %s"
    
    def getSelect(self, qp, iMortonRanges, xMortonRanges):
        queryArgs = ['SRID='+self.srid+';'+self.qp.wkt, ]
        query = ''
        
        zname = self.columnsNameDict['z'][0]
        kname = 'quadcellid'
        
        if len(iMortonRanges):
            if qp.queryType == 'nn':
                raise Exception('If using NN len(iMortonRanges) must be 0!')
            cols = dbops.getSelectCols(qp.columns, self.columnsNameDict, None, True)
            inMortonCondition = dbops.addMortonCondition(qp, iMortonRanges, kname, queryArgs) 
            inZCondition = dbops.addZCondition(qp, zname, queryArgs)
            query = "SELECT " + cols + " FROM (SELECT PC_Explode(pa) as qpoint from " + self.blockTable + dbops.getWhereStatement(inMortonCondition) + ") as qtable1 " + dbops.getWhereStatement(inZCondition) + " UNION "
        else:
            cols = dbops.getSelectCols(qp.columns, self.columnsNameDict, qp.statistics, True)
        
        mortonCondition = dbops.addMortonCondition(qp, xMortonRanges, kname, queryArgs)
        
        if qp.queryType in ('rectangle', 'circle', 'generic'):
            containsCondition = self.addContains(queryArgs)
            zCondition = dbops.addZCondition(qp, zname, queryArgs)
            query += "SELECT " + cols + " FROM (SELECT PC_Explode(PC_Intersection(pa,geom)) as qpoint from " + self.blockTable + ', (SELECT ST_GeomFromEWKT(%s) as geom) A ' + dbops.getWhereStatement([mortonCondition, containsCondition]) + ") as qtable2 " + dbops.getWhereStatement(zCondition)
        elif qp.queryType != 'nn':
            #Approximation
            query += "SELECT " + cols + " FROM (SELECT PC_Explode(pa) as qpoint from " + self.blockTable + ', (SELECT ST_GeomFromEWKT(%s) as geom) A ' + dbops.getWhereStatement(mortonCondition) + ") as qtable3 "
        return (query, queryArgs)
