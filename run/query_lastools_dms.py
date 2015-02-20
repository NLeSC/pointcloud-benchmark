#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, optparse, sys, multiprocessing, time, psycopg2, subprocess
import logging

TIMEFORMAT = "%Y/%m/%d/%H:%M:%S"
DB_DETAILS_FORMAT = '[username]:[password]:[host]:[port]:[dbname]'
DEFAULT_TABLE_NAME = 'EXTENTS'
QUERY_POLYGONS_TABLE = 'query_polygons'

def postgresConnectString(dbName = None, userName= None, password = None, dbHost = None, dbPort = None, cline = False, pgsql2shp = False):
    connString=''
    if cline:    
        if userName != None and userName != '':
            if pgsql2shp:
                connString += " -u " + userName
            else:
                connString += " -U " + userName
        if password != None and password != '':
            if pgsql2shp:
                connString += " -P " + password
            else:
                os.environ['PGPASSWORD'] = password
        if dbHost != None and dbHost != '':
            connString += " -h " + dbHost
        if dbPort != None and dbPort != '':
            connString += " -p " + dbPort
        if dbName != None and dbName != '':
            if pgsql2shp:            
                connString += " " + dbName
            else:
                connString += " -d " + dbName
    else:
        if dbName != None and dbName != '':
            connString += " dbname=" + dbName
        if userName != None and userName != '':
            connString += " user=" + userName
        if password != None and password != '':
            connString += " password=" + password
        if dbHost != None and dbHost != '':
            connString += " host=" + dbHost
        if dbPort != None and dbPort != '':
            connString += " port=" + dbPort
    
    return connString

def getLASParams(inputFile, bindir):
    outputLASInfo = subprocess.Popen(bindir + '/lasinfo -i ' + inputFile + ' -nc -nv -nco', shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    for line in outputLASInfo[1].split('\n'):
        if line.count('min x y z:'):
            [minX, minY, minZ] = line.split(':')[-1].strip().split(' ')
        elif line.count('max x y z:'):
            [maxX, maxY, maxZ] = line.split(':')[-1].strip().split(' ')
        elif line.count('number of point records:'):
            count = line.split(':')[-1].strip()
        elif line.count('scale factor x y z:'):
            [scaleX, scaleY, scaleZ] = line.split(':')[-1].strip().split(' ')
        elif line.count('offset x y z:'):
            [offsetX, offsetY, offsetZ] = line.split(':')[-1].strip().split(' ')
    return (count, minX, minY, minZ, maxX, maxY, maxZ, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ)

def createWKTRectGeometry(minx,miny,maxx,maxy):
    return 'POLYGON (('+str(minx)+' '+str(maxy)+','+str(minx)+' '+str(miny)+','+str(maxx)+' '+str(miny)+','+str(maxx)+' '+str(maxy)+','+str(minx)+' '+str(maxy)+'))'

def main(opts):
    # Set logging 
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt=TIMEFORMAT, level=logging.DEBUG)
    
    # Check the options
    if opts.input == '' or not os.path.exists(opts.input):
        logging.error('You must specify the folder with the LAS/LAZ data')
        return
    
    if opts.bindir == '' or not os.path.exists(opts.bindir):
        logging.error('You must specify a valid LASTools binary directory')
        return
    binaries = os.listdir(opts.bindir)
    for tool in ('lasinfo', 'lasclip.exe', 'lasmerge', 'las2las', 'lasindex', 'lassort.exe'):
        if tool not in binaries:
            logging.error(tool + ' is not found in ' + opts.bindir)
            return    
    
    if opts.output == '' or opts.output.split('.')[-1] not in ('las','laz'):
        logging.error('You must specify a valid output file (las or laz)')
        return
    
    if opts.srid == '':
        logging.error('Specify the SRID of your data')
        return
    try:
        srid = int(opts.srid)
    except:
        logging.error('Specify a valid SRID (integer)')
        return
    
    #Define the different connection parameters for psygopg2, psql and pgsqlshp
    if opts.db != '':
        (dbuser,dbpass,dbhost,dbport,dbname) = opts.db.split(':')
        connString = postgresConnectString(dbname, dbuser, dbpass, dbhost, dbport, False)
        clineConnString = postgresConnectString(dbname, dbuser, dbpass, dbhost, dbport, True)
        pgsql2shpConnString = postgresConnectString(dbname, dbuser, dbpass, dbhost, dbport, True, True)
    
    # Get the type of query (only one at a time is possible). In case of RECT and CIRC we also create a WKT to be used in the DB
    specFilters = 0
    queryType = ''
    if opts.rectangle != '':
        queryType = 'rectangle'
        (minx,miny,maxx,maxy) = opts.rectangle.split(':')
        specFilters += 1
        wkt = createWKTRectGeometry(minx, miny, maxx, maxy)    
    if opts.circle != '':
        queryType = 'circle'
        (cx,cy,rad) = opts.circle.split(':')
        specFilters += 1
        fcx = float(cx)
        fcy = float(cy)
        frad = float(rad)
        wkt = createWKTRectGeometry(fcx-frad, fcy-frad, fcx+frad, fcy+frad)
    if opts.polygon != '':
        if opts.db == '':
            logging.error('Polygon queries require the DB')
            return
        queryType = 'generic'
        lines = open(opts.polygon, 'r').read().split('\n')
        wkts = []
        for line in lines:
            if line != '':
                wkts.append(line)
        if len(wkts) > 1:
            logging.error('Filtering only possile for one polygon')
            return
        wkt = wkts[0]
        specFilters += 1
    if specFilters != 1:
        logging.error('You have to specify one (AND ONLY ONE) of the options rectangle, circle or polygon')
        return
    print wkt 
    # Get the z filters if provided
    zquery = ''    
    if (opts.minz != '') or (opts.maxz != ''):
        zconds = []
        if opts.minz != '':
            zconds.append(' -drop_z_below ' + str(opts.minz) + ' ')
        if opts.maxz != '':
            zconds.append(' -drop_z_above ' + str(opts.maxz) + ' ')
        zquery = ' '.join(zconds)
    
    t0 = time.time()
    inputList = str(opts.output) + '.list'
    if opts.db != '':
        connection = psycopg2.connect(connString)
        cursor = connection.cursor()
        
        tint = int(t0)
        
        # If not already there we create a table to hold the geometry that will be used to filter list of files that overlap which overlap with it (using PostGIS intersection methods) 
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ( QUERY_POLYGONS_TABLE, ))
        if not cursor.fetchone()[0]:
            cursor.execute("CREATE TABLE " +  QUERY_POLYGONS_TABLE + " (t integer, geom public.geometry(Geometry," + opts.srid + "));")
            connection.commit()
        # Insert the geometry as WKT
        cursor.execute("INSERT INTO " + QUERY_POLYGONS_TABLE + " VALUES (%s,ST_GeomFromEWKT(%s))", [tint, 'SRID='+opts.srid+';'+wkt])
        connection.commit()
        
        if queryType == 'generic':
            # Using PostGIS tool pgsql2shp we create a ShapeFile which is required by lasclip.exe 
            precommand = 'pgsql2shp -f ' + str(opts.output) + '.shp '+ pgsql2shpConnString +' "select ST_SetSRID(geom, ' + opts.srid + ') from ' + QUERY_POLYGONS_TABLE + ' where t = ' + str(tint) + ';"'
            logging.info(precommand)
            os.system(precommand)
        # Create the list of files that intersect with the geometry: rectangle, circle or generic.
        # Note that in the case of the circle we use the rectangle that contains the circle (faster to build than creating a circle)
        prec = 'psql ' + clineConnString + ' -t -A -c "select filepath from ' + opts.table + ',' + QUERY_POLYGONS_TABLE + ' where ST_Intersects( ' + QUERY_POLYGONS_TABLE + '.geom, ' + opts.table + '.geom ) and ' + QUERY_POLYGONS_TABLE + '.t = ' + str(tint) + '" > ' + inputList
        logging.info(prec)
        os.system(prec)
    else:
        # If no DB is provided we use all the files as input of lasmerge or lasclip.exe
        os.system('ls ' + opts.input + "/* | grep -E 'las|laz' > " + inputList)
    
    if queryType in ('rectangle', 'circle'):
        if queryType == 'rectangle':
            command = opts.bindir + '/lasmerge -lof ' + inputList + ' -inside ' + str(minx) + ' ' + str(miny) + ' ' + str(maxx) + ' ' + str(maxy) + zquery + ' -o ' + opts.output
        else:
            command = opts.bindir + '/lasmerge -lof ' + inputList + ' -inside_circle ' + str(cx) + ' ' + str(cy) + ' ' + str(rad) + zquery + ' -o ' + opts.output
    elif queryType == 'generic':
        command = opts.bindir + '/lasclip.exe -lof ' + inputList + ' -poly ' + str(opts.output) + '.shp ' + zquery + ' -o ' + opts.output
        if int(os.popen('wc -l ' + inputList).read().split()[0]) > 1:
            command += ' -merged'                    
            
    logging.info(command)
    os.system(command)
    
    eTime = time.time() - t0
    npoints = getLASParams(opts.output, opts.bindir)[0]
    
    logging.info('Output file ' + opts.output + (' generated in %.2f seconds' % (eTime)) + '. Number of points: ' + str(npoints))    
                
if __name__ == "__main__":
    usage = 'Usage: %prog [options]'
    description = "Filter points in a rectangle, circle or polygon (with some noise in the case of the polygon) in LAS/LAZ files. It is recommended to have prepared the data using the prepare_lastools_dms.py"
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--input',default='',help='The directory with the LAS/LAZ data', type='string')
    op.add_option('-l','--bindir',default='',help='LASTools bin directory', type='string')
    op.add_option('-d','--db',default='',help='Connection string of the DB in the form of ' + DB_DETAILS_FORMAT + '. If not specified not DB will be used to speed up queries. In the case of query polygons the DB MUST be provided', type='string')
    op.add_option('-t','--table',help='Table name to be used in the DB [default ' + DEFAULT_TABLE_NAME + ']', type='string', default=DEFAULT_TABLE_NAME)
    op.add_option('-r','--srid',default='',help='SRID of the data', type='string')
    op.add_option('-o','--output',default='',help='The output file (LAS or LAZ) with the filtered points', type='string')
    op.add_option('-p','--polygon',default='',help='File with WKT representation of the polygon. The output file will contain points in the given polygon', type='string')
    op.add_option('-s','--rectangle',default='',help='[minx]:[miny]:[maxx]:[maxy]. The output file will contain points in the rectangle', type='string')
    op.add_option('-c','--circle',default='',help='[cx]:[cy]:[rad]. The output file will contain points in the circle', type='string')
    op.add_option('--minz',default='',help='Filter points below certain elevation (this can be used in combination with polygon, rectangle or circle)', type='string')
    op.add_option('--maxz',default='',help='Filter points above certain elevation (this can be used in combination with polygon, rectangle or circle)', type='string')
    (opts, args) = op.parse_args() 
    main(opts)
