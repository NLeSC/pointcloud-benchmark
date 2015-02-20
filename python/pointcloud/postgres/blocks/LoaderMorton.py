#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.postgres.AbstractLoader import AbstractLoader
from pointcloud import utils

class LoaderMorton(AbstractLoader):
    def initialize(self):
        self.createDB()
        self.initPointCloud()
        self.createBlocks(self.blockTable, quadcell = True)
        self.createQuadCellId(cursor)
        
    def loadFromFile(self, index, fileAbsPath):
        (self.dimensionsNames, pcid) = self.addPCFormat(self.schemaFile, fileAbsPath)
        columns = []
        for dimName in self.dimensionsNames:
            for col in self.colsData:
                if self.colsData[col][-1] == dimName:
                    columns.append(col)
        # Add the morton2D code
        colsWithk = columns[:]
        colsWithk.append('k')
        
        flatTable = self.blockTable + '_temp_' + str(index)
        cols = ''.join(colsWithk)
        self.createFlat(flatTable,cols)
        c = 'las2pg -s '+ fileAbsPath +' --stdout --parse ' + cols + ' | psql '+ self.connectString(False, True) +' -c "copy '+ flatTable +' from stdin with binary"'
        logging.debug(c)
        os.system(c)
        query = """INSERT INTO """ + self.blockTable + """ (pa,quadcellid)
SELECT PC_Patch(pt),quadCellId FROM (SELECT PC_MakePoint(%s, ARRAY[x,y,z]) pt, quadCellId(morton2D,%s) as quadCellId FROM """ + flatTable + """) A GROUP BY quadCellId"""
        blockQuadTreeLevel = 21
        queryArgs = [pcid, blockQuadTreeLevel]
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute(query, queryArgs)
        connection.commit()
        cursor.execute('DROP TABLE ' + flatTable)
        connection.commit()

    def close(self):
        self.indexClusterVacuumBlock(self.blockTable, quadcell = True)
        

    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)