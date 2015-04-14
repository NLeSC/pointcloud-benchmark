#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud.AbstractQuerier import AbstractQuerier as AQuerier
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB
from pointcloud import monetdbops

class AbstractQuerier(AQuerier, CommonMonetDB):
    """Abstract class for the queriers to be implemented for each different 
    solution for the benchmark"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AQuerier.__init__(self, configuration)
        self.setVariables(configuration)
        
    def close(self):
        return
    
    def prepareQuery(self, cursor, queryId, queriesParameters, addGeom = False):
        self.queryIndex = int(queryId)
        self.resultTable = 'query_results_' + str(self.queryIndex)
        self.qp = queriesParameters.getQueryParameters('mon', queryId, self.colsData.keys())
        logging.debug(self.qp.queryKey)
        
        if addGeom:
            # We insert the polygon in the DB
            cursor.execute("INSERT INTO " + utils.QUERY_TABLE + " VALUES (%s,GeomFromText(%s,%s))", [self.queryIndex, self.qp.wkt, self.srid])
            cursor.connection.commit()

    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        queryArgs.extend([self.queryIndex, ])
        return (utils.QUERY_TABLE, "id = %s AND contains(geom, " + xname +"," + yname + ")")
