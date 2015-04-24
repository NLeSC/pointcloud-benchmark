#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time, logging
from pointcloud.monetdb.AbstractQuerier import AbstractQuerier
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB
from pointcloud import dbops, monetdbops, utils

class Querier(AbstractQuerier, CommonMonetDB):
    """MonetDB querier"""
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
        
        t0 = time.time()
        (query, queryArgs) = dbops.getSelect(self.qp, self.flatTable, self.addContainsCondition, self.DM_FLAT)
        
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
