#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import logging,os
from pointcloud import utils, lasops

#
# This module contains methods that use PDAL or are useful for PDAL
#

def OracleWriter(inputFileAbsPath, connectionString, dimensionsNames, blockTable, baseTable, srid, blockSize):
    (_, _, _, _, _, _, _, _, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = lasops.getPCFileDetails(inputFileAbsPath)  
    """ Create a XML file to load the data, in the given file, into the DB """
    xmlContent = """
<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
 <Writer type="writers.oci">
   <Option name="debug">false</Option>
   <Option name="verbose">1</Option>
   <Option name="connection">""" + connectionString + """</Option>
   <Option name="base_table_name">""" + baseTable + """</Option>
   <Option name="block_table_name">""" + blockTable + """</Option>
   <Option name="store_dimensional_orientation">true</Option>
   <Option name="cloud_column_name">pc</Option>
   <Option name="is3d">false</Option>
   <Option name="solid">false</Option>
   <Option name="overwrite">false</Option>
   <Option name="disable_cloud_trigger">true</Option>
   <Option name="srid">""" + str(srid) + """</Option>
   <Option name="create_index">false</Option>
   <Option name="capacity">""" + str(blockSize) + """</Option>
   <Option name="stream_output_precision">8</Option>
   <Option name="pack_ignored_fields">true</Option>
   <Option name="output_dims">""" + ",".join(dimensionsNames) + """</Option>
   <Option name="offset_x">""" + str(offsetX) + """</Option>
   <Option name="offset_y">""" + str(offsetY) + """</Option>
   <Option name="offset_z">""" + str(offsetZ) + """</Option>
   <Option name="scale_x">""" + str(scaleX) + """</Option>
   <Option name="scale_y">""" + str(scaleY) + """</Option>
   <Option name="scale_z">""" + str(scaleZ) + """</Option>
   <Filter type="filters.chipper">
     <Option name="capacity">""" + str(blockSize) + """</Option>
     <Reader type="readers.las">
       <Option name="filename">""" + inputFileAbsPath + """</Option>
       <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
     </Reader>
   </Filter>            
 </Writer>
</Pipeline>      
"""
    outputFileName = os.path.basename(inputFileAbsPath) + '.xml'
    utils.writeToFile(outputFileName, xmlContent)
    return outputFileName

def OracleReaderLAS(outputFileAbsPath, connectionString, blockTable, baseTable, srid, wkt):
    xmlContent = """
<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
  <Writer type="writers.las">
    <Option name="filename">""" + outputFileAbsPath + """</Option>
    <Filter type="filters.crop">
        <Option name="polygon">""" + wkt + """</Option>
        <Reader type="readers.oci">
          <Option name="query">
SELECT l."OBJ_ID", l."BLK_ID", l."BLK_EXTENT",
       l."BLK_DOMAIN", l."PCBLK_MIN_RES",
       l."PCBLK_MAX_RES", l."NUM_POINTS",
       l."NUM_UNSORTED_POINTS", l."PT_SORT_DIM",
       l."POINTS", b.pc
FROM """ + blockTable + """ l, """ + baseTable + """ b
WHERE
    l.obj_id = b.id
    AND
    SDO_FILTER(l.blk_extent,SDO_GEOMETRY('""" + wkt + """', """ + str(srid) + """)) = 'TRUE'
          </Option>
          <Option name="connection">""" + connectionString + """</Option>
          <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
        </Reader>
    </Filter>
  </Writer>
</Pipeline>
"""
    outputFileName = os.path.basename(outputFileAbsPath) + '.xml'
    utils.writeToFile(outputFileName, xmlContent)
    return outputFileName

def OracleReaderStdOut(connectionString, blockTable, baseTable, srid, wkt):
    xmlContent = """
<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
  <Writer type="writers.text">
    <Option name="filename">STDOUT</Option>
    <Option name="order">X,Y,Z</Option>
    <Filter type="filters.crop">
        <Option name="polygon">""" + wkt + """</Option>
        <Reader type="readers.oci">
          <Option name="query">
    SELECT l."OBJ_ID", l."BLK_ID", l."BLK_EXTENT",
           l."BLK_DOMAIN", l."PCBLK_MIN_RES",
           l."PCBLK_MAX_RES", l."NUM_POINTS",
           l."NUM_UNSORTED_POINTS", l."PT_SORT_DIM",
           l."POINTS", b.pc
    FROM """ + blockTable + """ l, """ + baseTable + """ b
    WHERE
        l.obj_id = b.id
        AND
        SDO_FILTER(l.blk_extent,SDO_GEOMETRY('""" + wkt + """', """ + str(srid) + """)) = 'TRUE'
          </Option>
              <Option name="connection">""" + connectionString + """</Option>
              <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
        </Reader>
    </Filter>
  </Writer>
</Pipeline>
"""

def PostgreSQLWriter(inputFileAbsPath, connectionString, pcid, dimensionsNames, blockTable, srid, blockSize, compression):
    """ Create a XML file to load the data, in the given file, into the DB """
    (_, _, _, _, _, _, _, _, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = lasops.getPCFileDetails(inputFileAbsPath)  

    xmlContent = """<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
<Writer type="writers.pgpointcloud">
    <Option name="connection">""" + connectionString + """</Option>
    <Option name="table">""" + blockTable + """</Option>
    <Option name="column">pa</Option>
    <Option name="srid">""" + str(srid) + """</Option>
    <Option name="pcid">""" + str(pcid) + """</Option>
    <Option name="overwrite">false</Option>
    <Option name="capacity">""" + str(blockSize) + """</Option>
    <Option name="compression">""" + compression + """</Option>
    <Option name="output_dims">""" + ",".join(dimensionsNames) + """</Option>
    <Option name="offset_x">""" + str(offsetX) + """</Option>
    <Option name="offset_y">""" + str(offsetY) + """</Option>
    <Option name="offset_z">""" + str(offsetZ) + """</Option>
    <Option name="scale_x">""" + str(scaleX) + """</Option>
    <Option name="scale_y">""" + str(scaleY) + """</Option>
    <Option name="scale_z">""" + str(scaleZ) + """</Option>
    <Filter type="filters.chipper">
        <Option name="capacity">""" + str(blockSize) + """</Option>
        <Reader type="readers.las">
            <Option name="filename">""" + inputFileAbsPath + """</Option>
            <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
        </Reader>
    </Filter>
</Writer>
</Pipeline>
"""
    outputFileName = os.path.basename(inputFileAbsPath) + '.xml'
    utils.writeToFile(outputFileName, xmlContent)
    return outputFileName

def PostgresSQLReaderLAS(outputFileAbsPath, connectionString, blockTable, srid, wkt):
    xmlContent = """
<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
  <Writer type="writers.las">
    <Option name="filename">""" + outputFileAbsPath + """</Option>
    <Filter type="filters.crop">
        <Option name="polygon">""" + wkt + """</Option>
        <Reader type="readers.pgpointcloud">
          <Option name="connection">""" + connectionString + """</Option>
          <Option name="table">""" + blockTable + """</Option>
          <Option name="column">pa</Option>
          <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
          <Option name="where">
            PC_Intersects(pa, ST_GeomFromEWKT('SRID=""" + str(srid) + """;""" + wkt + """'))
          </Option>
        </Reader>
    </Filter>
  </Writer>
</Pipeline>
"""
    outputFileName = os.path.basename(outputFileAbsPath) + '.xml'
    utils.writeToFile(outputFileName, xmlContent)
    return outputFileName

def PostgresSQLReaderStdOut(connectionString, blockTable, srid, wkt):
    xmlContent = """
<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
  <Writer type="writers.text">
    <Option name="filename">STDOUT</Option>
    <Option name="order">X,Y,Z</Option>
    <Filter type="filters.crop">
        <Option name="polygon">""" + wkt + """</Option>
        <Reader type="readers.pgpointcloud">
          <Option name="connection">""" + connectionString + """</Option>
          <Option name="table">""" + blockTable + """</Option>
          <Option name="column">pa</Option>
          <Option name="spatialreference">EPSG:""" + str(srid) + """</Option>
          <Option name="where">
            PC_Intersects(pa, ST_GeomFromEWKT('SRID=""" + str(srid) + """;""" + wkt + """'))
          </Option>
        </Reader>
    </Filter>
  </Writer>
</Pipeline>
"""

def executePDALCount(xmlFile):
    command = 'pdal pipeline ' + xmlFile + ' | wc -l'
    result = utils.shellExecute(command).replace('\n','')
    try:
        result  = int(result) - 0
    except:
        result = -1
    return result

def executePDAL(xmlFile):
    c = 'pdal pipeline ' + xmlFile
    logging.debug(c)
    os.system(c)
    # remove the XML file
    #os.system('rm ' + xmlFile)
