#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud.AbstractQuerier import AbstractQuerier as AQuerier
from pointcloud.postgres.CommonPostgres import CommonPostgres
from pointcloud import postgresops

class AbstractQuerier(AQuerier, CommonPostgres):
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AQuerier.__init__(self, configuration)
        self.setVariables(configuration)
        
        self.queryIndex = None
        self.resultTable = None
        self.qp = None
        self.srid = None #to be filled in by the different implementations (in their initialize method)

    def close(self):
        return
    
    def prepareQuery(self, cursor, queryId, queriesParameters, addGeom = False):
        self.queryIndex = int(queryId)
        self.resultTable = 'query_results_' + str(self.queryIndex)
        
        self.qp = queriesParameters.getQueryParameters('psql',queryId)
        logging.debug(self.qp.queryKey)
        
        if addGeom:
            cursor.execute('insert into ' + self.queryTable + ' values (%s,ST_GeomFromEWKT(%s))', [self.queryIndex,'SRID='+str(self.srid)+';'+self.qp.wkt])
            cursor.connection.commit()

    def getBBoxGeometry(self, cursor, table, qId):
        cursor.execute('select st_xmin(geom), st_xmax(geom), st_ymin(geom), st_ymax(geom) from ' + table + ' where id = %s', [qId,])
        return cursor.fetchone()
    
    def createGridTableMethod(self, cursor, gridTable, nrows, ncols):
        (minx,maxx,miny,maxy) = self.getBBoxGeometry(cursor, self.queryTable, self.queryIndex)
        query = """ 
CREATE TABLE """ + gridTable + """ AS
    SELECT row_number() OVER() AS id, ST_Intersection(A.geom, ST_SetSRID(B.geom, %s)) as geom FROM (SELECT geom FROM """ + self.queryTable + """ WHERE id = %s) A, ST_CreateFishnet(%s, %s, %s, %s, %s, %s) B 
"""
        queryArgs = [self.srid, self.queryIndex, nrows, ncols, (maxx - minx) / float(ncols), (maxy- miny) /  float(nrows), minx, miny,]
        postgresops.mogrifyExecute(cursor, query, queryArgs)
        cursor.execute("CREATE INDEX " + gridTable + "_rowcol ON " + gridTable + " ( id )")
        cursor.connection.commit()
