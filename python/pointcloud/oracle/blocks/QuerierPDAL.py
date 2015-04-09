#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time
from pointcloud import lasops, pdalxml, oracleops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):    
    def __init__(self, configuration):
        """ Set configuration parameters and create user if required """
        AbstractQuerier.__init__(self, configuration)
        # Create the quadtree
        connection = self.getConnection()
        cursor = connection.cursor()
        
        oracleops.mogrifyExecute(cursor, "SELECT srid FROM user_sdo_geom_metadata WHERE table_name = '" + self.blocksTable + "'")
        (self.srid,) = cursor.fetchone()[0]
        
    def queryDisk(self, queryId, iterationId, queriesParameters):
        self.prepareQuery(queryId, queriesParameters, False)
        outputFileAbsPath = 'output' +  str(queryIndex) + '.las'
        xmlFile = pdalxml.OracleReader(fileAbsPath, self.connectString(), self.blockTable, self.baseTable, self.srid, self.qp.wkt)
        t0 = time.time()
        c = 'pdal pipeline ' + xmlFile + ' -d -v 6'
        logging.debug(c)
        os.system(c)
        eTime = time.time() - t0
        numPoints = lasops.getNumPoints(outputFileAbsPath)
        os.system('rm ' + xmlFile)
        os.system('rm ' + outputFileAbsPath)
        return (eTime, numPoints)