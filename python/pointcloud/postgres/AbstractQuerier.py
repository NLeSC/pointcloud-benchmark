#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
import psycopg2
from pointcloud.AbstractQuerier import AbstractQuerier as AQuerier
from pointcloud.postgres.CommonPostgres import CommonPostgres
from pointcluod import postgresops

class AbstractQuerier(AQuerier, CommonPostgres):
    """Abstract class for the queriers to be implemented for each different 
    solution for the benchmark"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AQuerier.__init__(self, configuration)
        self.setVariables(configuration)

    def connect(self, superUser = False):
        return psycopg2.connect(self.connectString(superUser))
    
    def initialize(self):
        #Variables used during query
        self.queryIndex = None
        self.resultTable = None
        self.qp = None
        
        # Create a table to store the query geometries
        connection = self.connect()
        cursor = connection.cursor()
        
        postgresops.dropTable(cursor, self.queryTable, check = True)
        postgresops.mogrifyExecute(cursor, "CREATE TABLE " +  self.queryTable + " (id integer, geom public.geometry(Geometry," + self.srid + "));")
        
        connection.commit()
        connection.close()
        
    def close(self):
        return
    
    def prepareQuery(self, queryId, queriesParameters, cursor, firstQuery = True):
        self.queryIndex = int(queryId)
        self.resultTable = 'query_results_' + str(self.queryIndex)
        
        self.qp = queriesParameters.getQueryParameters('psql',queryId, self.colsData.keys())
        logging.debug(self.qp.queryKey)

    def getBBoxGeometry(self, cursor, table, qId):
        cursor.execute('select st_xmin(geom), st_xmax(geom), st_ymin(geom), st_ymax(geom) from ' + table + ' where id = %s', [qId,])
        return cursor.fetchone()
    
    def createGridTableMethod(self, cursor, gridTable, nrows, ncols):
        postgresops.mogrifyExecute(cursor, "SELECT st_xmin(geom), st_xmax(geom), st_ymin(geom), st_ymax(geom) FROM (SELECT ST_GeomFromEWKT(%s) as geom) A", ['SRID='+self.srid+';'+self.qp.wkt, ])
        (minx,maxx,miny,maxy) = cursor.fetchone()
        query = """ 
CREATE TABLE """ + gridTable + """ AS
    SELECT row_number() OVER() AS id, ST_Intersection(A.geom, ST_SetSRID(B.geom, %s)) as geom FROM (SELECT ST_GeomFromEWKT(%s) as geom) A, ST_CreateFishnet(%s, %s, %s, %s, %s, %s) B 
"""
        queryArgs = [self.srid, 'SRID='+self.srid+';'+self.qp.wkt, nrows, ncols, (maxx - minx) / float(ncols), (maxy- miny) /  float(nrows), minx, miny,]
        postgresops.mogrifyExecute(cursor, query, queryArgs)
        cursor.execute("CREATE INDEX " + gridTable + "_rowcol ON " + gridTable + " ( id )")
        cursor.connection.commit()
