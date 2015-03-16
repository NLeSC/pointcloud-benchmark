#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import time
from pointcloud import lastoolsops
from pointcloud.oracle.AbstractQuerier import AbstractQuerier

class QuerierPDAL(AbstractQuerier):        
    def query(self, queryId, iterationId, queriesParameters):
        self.prepareQuery(queryId, queriesParameters, False)
        outputFileAbsPath = 'output' +  str(queryIndex) + '.las'
        xmlFile = pdal_xml.OracleReader(fileAbsPath, self.connectString(), self.blockTable, self.baseTable, self.srid, self.wkt)
        t0 = time.time()
        c = 'pdal pipeline ' + xmlFile + ' -d -v 6'
        logging.debug(c)
        os.system(c)
        eTime = time.time() - t0
        numPoints = lastoolsops.getNumPoints(outputFileAbsPath)
        os.system('rm ' + xmlFile)
        os.system('rm ' + outputFileAbsPath)
        return (eTime, numPoints)