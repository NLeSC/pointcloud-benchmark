#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.postgres.AbstractLoader import AbstractLoader
from pointcloud import utils

BLOCKQUADTREELEVEL = 21

class LoaderMorton(AbstractLoader):
    def initialize(self):
        # Initialize DB and extensions if creation of user is required
        if self.cDB:
            self.createDB()
            self.initPointCloud()
        # Create the blocks table 
        self.createBlocksTable(self.blockTable, self.tableSpace, True)
        self.createQuadCellId(cursor)
        
        logging.info('Getting files, extent and SRID from input folder ' + self.inputFolder)
        (self.inputFiles, self.srid, _, self.minX, self.minY, _, self.maxX, self.maxY, _, self.scaleX, self.scaleY, _) = lasops.getPCFolderDetails(self.inputFolder)

    def process(self):
        logging.info('Starting data loading with las2pg (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)

    def loadFromFile(self, index, fileAbsPath):
        (dimensionsNames, pcid, compression) = self.addPCFormat(self.schemaFile, fileAbsPath)
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
        self.createFlatTable(flatTable, self.indexTableSpace, cols) # use index table space for temporal table
        self.loadFromBinaryLoader(self.getConnectionString(False, True), flatTable, fileAbsPath, cols, self.minX, self.minY, self.scaleX, self.scaleY)
        query = """INSERT INTO """ + self.blockTable + """ (pa,quadcellid)
SELECT PC_Patch(pt),quadCellId FROM (SELECT PC_MakePoint(%s, ARRAY[x,y,z]) pt, quadCellId(morton2D,%s) as quadCellId FROM """ + flatTable + """) A GROUP BY quadCellId"""
        queryArgs = [pcid, BLOCKQUADTREELEVEL]
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute(query, queryArgs)
        connection.commit()
        postgresops.dropTable(cursor, flatTable)

    def close(self):
        self.indexBlockTable(self.blockTable, self.indexTableSpace, True, self.cluster)
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)