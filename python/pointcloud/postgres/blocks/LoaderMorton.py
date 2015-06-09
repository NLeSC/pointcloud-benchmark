#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud.postgres.AbstractLoader import AbstractLoader
from pointcloud import utils, lasops, postgresops

BLOCKQUADTREELEVEL = 21

class LoaderMorton(AbstractLoader):
    def initialize(self):
        # Initialize DB and extensions if creation of user is required
        if self.cDB:
            self.createDB()
            connection = self.getConnection()
            cursor = connection.cursor()
            self.initPointCloud(cursor)
        else:
            connection = self.getConnection()
            cursor = connection.cursor()
        # Create SQL method to get the quad cell id
        self.createQuadCellId(cursor)
        
        logging.info('Getting files and extent from input folder ' + self.inputFolder)
        self.numProcessesLoad = int(os.popen('grep -c ^processor /proc/cpuinfo').read())
        (self.inputFiles, _, _, self.minX, self.minY, self.minZ, self.maxX, self.maxY, _, self.scaleX, self.scaleY, self.scaleZ) = lasops.getPCFolderDetails(self.inputFolder, numProc = self.numProcessesLoad)
        self.createBlocksTable(cursor, self.blockTable, self.tableSpace, True)
        
        # Create meta table to save the extent of the PC
        self.metaTable = self.blockTable + '_meta'
        self.createMetaTable(cursor, self.metaTable)

        connection.close()

    def process(self):
        logging.info('Starting data loading with las2pg (parallel by python) from ' + self.inputFolder + ' to ' + self.dbName)
        # Insert the extent of the loaded PC
        metaArgs = (self.blockTable, self.srid, self.minX, self.minY, self.maxX, self.maxY, self.scaleX, self.scaleY)
        connection = self.getConnection()
        cursor = connection.cursor()
        postgresops.mogrifyExecute(cursor, "INSERT INTO " + self.metaTable + " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" , metaArgs)
        connection.close()
        # Start the multiprocessing (las2pg in parallel)
        return self.processMulti(self.inputFiles, self.numProcessesLoad, self.loadFromFile)

    def loadFromFile(self, index, fileAbsPath):
        connection = self.getConnection()
        cursor = connection.cursor()
        # Add PC format to pointcloud_formats
        (columns, pcid, compression) = self.addPCFormat(cursor, self.schemaFile, fileAbsPath, self.srid)
        # Add the morton2D code to the requeste columns
        columns.append('k')
        # Create a temporal flat table and load the points to it
        flatTable = self.blockTable + '_temp_' + str(index)
        self.createFlatTable(cursor, flatTable, self.indexTableSpace, columns) # use index table space for temporal table
        self.loadFromBinaryLoader(self.getConnectionString(False, True), flatTable, fileAbsPath, columns, self.minX, self.minY, self.scaleX, self.scaleY)
        # Create the blocks by grouping points in QuadTree cells
        query = """INSERT INTO """ + self.blockTable + """ (pa,quadcellid)
SELECT PC_Patch(pt),quadCellId FROM (SELECT PC_MakePoint(%s, ARRAY[x,y,z]) pt, quadCellId(morton2D,%s) as quadCellId FROM """ + flatTable + """) A GROUP BY quadCellId"""
        queryArgs = [pcid, BLOCKQUADTREELEVEL]
        cursor.execute(query, queryArgs)
        connection.commit()
        # Drop the temporal table
        postgresops.dropTable(cursor, flatTable)

    def close(self):
        connection = self.getConnection()
        cursor = connection.cursor()
        self.indexBlockTable(cursor, self.blockTable, self.indexTableSpace, True, self.cluster)
        connection.close()
        
    def getNumPoints(self):
        return self.getNumPointsBlocks(self.blockTable)
