import utils

def OracleWriter(inputFileAbsPath, connectionString, dimensionsNames, blockTable, baseTable, srid, blockSize, offsets, scales):
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
   <Option name="capacity">""" + blockSize + """</Option>
   <Option name="stream_output_precision">8</Option>
   <Option name="pack_ignored_fields">true</Option>
   <Option name="output_dims">""" + ",".join(dimensionsNames) + """</Option>
   <Option name="offset_x">""" + offsets['X'] + """</Option>
   <Option name="offset_y">""" + offsets['Y'] + """</Option>
   <Option name="offset_z">""" + offsets['Z'] + """</Option>
   <Option name="scale_x">""" + scales['X'] + """</Option>
   <Option name="scale_y">""" + scales['Y'] + """</Option>
   <Option name="scale_z">""" + scales['Z'] + """</Option>
   <Filter type="filters.chipper">
     <Option name="capacity">""" + blockSize + """</Option>
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
    SDO_FILTER(l.blk_extent,SDO_GEOMETRY('""" + wkt + """', """ + srid + """)) = 'TRUE'
          </Option>
          <Option name="connection">""" + connectionString + """</Option>
          <Option name="spatialreference">EPSG:""" + srid + """</Option>
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
    SDO_FILTER(l.blk_extent,SDO_GEOMETRY('""" + wkt + """', """ + srid + """)) = 'TRUE'
      </Option>
          <Option name="connection">""" + connectionString + """</Option>
          <Option name="spatialreference">EPSG:""" + srid + """</Option>
    </Reader>
  </Writer>
</Pipeline>

"""

def PostgreSQLWriter(inputFileAbsPath, connectionString, pcid, dimensionsNames, blockTable, srid, blockSize, compression, offsets, scales):
    """ Create a XML file to load the data, in the given file, into the DB """

    xmlContent = """<?xml version="1.0" encoding="utf-8"?>
<Pipeline version="1.0">
<Writer type="writers.pgpointcloud">
    <Option name="connection">""" + connectionString + """</Option>
    <Option name="table">""" + blockTable + """</Option>
    <Option name="column">pa</Option>
    <Option name="srid">""" + str(srid) + """</Option>
    <Option name="pcid">""" + str(pcid) + """</Option>
    <Option name="overwrite">false</Option>
    <Option name="capacity">""" + blockSize + """</Option>
    <Option name="compression">""" + compression + """</Option>
    <Option name="output_dims">""" + ",".join(dimensionsNames) + """</Option>
    <Option name="offset_x">""" + offsets['X'] + """</Option>
    <Option name="offset_y">""" + offsets['Y'] + """</Option>
    <Option name="offset_z">""" + offsets['Z'] + """</Option>
    <Option name="scale_x">""" + scales['X'] + """</Option>
    <Option name="scale_y">""" + scales['Y'] + """</Option>
    <Option name="scale_z">""" + scales['Z'] + """</Option>
    <Filter type="filters.chipper">
        <Option name="capacity">""" + blockSize + """</Option>
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
