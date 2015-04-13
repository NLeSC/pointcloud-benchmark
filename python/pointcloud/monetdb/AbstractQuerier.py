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

    def initialize(self):
        #Variables used during query
        self.queryIndex = None
        self.resultTable = None
        self.qp = None
        connection = self.getConnection()
        cursor = connection.cursor()
        logging.info('Getting SRID and extent from ' + self.dbName)
        monetdbops.mogrifyExecute(cursor, "SELECT srid, minx, miny, maxx, maxy, scalex, scaley from " + self.metaTable)
        (self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY) = cursor.fetchone()[0]
        
        connection.close()
        
    def close(self):
        return
    
    def prepareQuery(self, queryId, queriesParameters):
        self.queryIndex = int(queryId)
        self.resultTable = 'query_results_' + str(self.queryIndex)
        self.qp = queriesParameters.getQueryParameters('mon', queryId, self.colsData.keys())
        logging.debug(self.qp.queryKey)

    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        #queryArgs.extend([self.queryIndex, ])
        return (" contains(GeomFromText('" + self.qp.wkt + "','" + self.srid + "'), " + xname +"," + yname + ")", None)
