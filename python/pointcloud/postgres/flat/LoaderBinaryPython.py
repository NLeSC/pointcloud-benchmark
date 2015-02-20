#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, logging
from pointcloud import utils
from pointcloud.postgres.AbstractLoader import AbstractLoader
from liblas import file
from io import BytesIO
from struct import pack


class LoaderBinaryPython(AbstractLoader):
    def initialize(self):
        self.createDB()
        self.createFlat(self.flatTable,self.columns)
        
    def loadFromFile(self, index, fileAbsPath):
        connection = self.connect()
        cursor = connection.cursor()
        
        f = file.File(fileAbsPath, mode='r')
        
        # Make a binary file object for COPY FROM
        cpy = BytesIO()
        # 11-byte signature, no flags, no header extension
        cpy.write(pack('!11sii', b'PGCOPY\n\377\r\n\0', 0, 0)) #!  is for netwoork format (big-endian), 11 is number of bytes of string, s is for string, i integer

        for p in f:
            cpy.write(pack('!h', len(self.columns)))
            for c in self.columns:
                (dname, dtypefull, dtypeshort, dsize, dattr) = self.colsData[c][0:5]
                value = p.__getattribute__(dattr)
                if dname in ('R','G','B'):
                    if dname == 'R':
                        value = value.red
                    elif dname == 'G':
                        value = value.green
                    else:
                        value = value.blue
                elif dname in ('ux','uy','uz'):
                    value = int(value /  f.header.scale[('ux','uy','uz').index(dname)])
                elif dname == 'morton2D':
                    raise Exception('Morton code not supported yet!')
                cpy.write(pack('!i' + dtypeshort, dsize, value))
                
        
        # File trailer
        cpy.write(pack('!h', -1))
        
        # Copy data to database
        cpy.seek(0)
        cursor.copy_expert("COPY " + self.flatTable + " FROM STDIN WITH BINARY", cpy)
        logging.debug("COPY " + self.flatTable + " FROM '" + fileAbsPath + "' WITH BINARY")
        connection.commit()
        connection.close()
 
    def close(self):
        self.indexClusterVacuumFlat(self.flatTable, self.index)
        
    def getNumPoints(self):
        return self.getNumPointsFlat(self.flatTable)
