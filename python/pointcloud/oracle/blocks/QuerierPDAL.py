#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time
from pointcloud import lasops, pdalxml
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):        
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