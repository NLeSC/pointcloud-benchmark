#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging
from pointcloud.AbstractQuerier import AbstractQuerier as AQuerier
from pointcloud.monetdb.CommonMonetDB import CommonMonetDB

class AbstractQuerier(AQuerier, CommonMonetDB):
    """Abstract class for the queriers to be implemented for each different 
    solution for the benchmark"""
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AQuerier.__init__(self, configuration)
        self.setVariables(configuration)

    def connect(self, superUser = False):
        return self.connection()
    
    def initialize(self):
        #Variables used during query
        self.queryIndex = None
        self.resultTable = None
        self.qp = None
        
    def close(self):
        return
    
    def prepareQuery(self, queryId, queriesParameters):
        self.queryIndex = int(queryId)
        self.resultTable = 'query_results_' + str(self.queryIndex)
        
        self.qp = queriesParameters.getQueryParameters('mon',queryId, self.colsData.keys())
        self.wkt = queriesParameters.getWKT(queriesParameters.getQuery(queryId))
        logging.debug(self.qp.queryKey)

    def addContainsCondition(self, queryParameters, queryArgs, xname, yname):
        #queryArgs.extend([self.queryIndex, ])
        return (" contains(GeomFromText('" + self.wkt + "','" + self.srid + "'), " + xname +"," + yname + ")", None)
        #return (" id = %s AND contains(geom, Point(x,y))", self.queryTable)    
