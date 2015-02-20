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

def postgresConnectString(dbName = None, userName= None, password = None, dbHost = None, dbPort = None, cline = False):
    connString=''
    if cline:    
        if dbName != None and dbName != '':
            connString += " " + dbName
        if userName != None and userName != '':
            connString += " -U " + userName
        if password != None and password != '':
            os.environ['PGPASSWORD'] = password
        if dbHost != None and dbHost != '':
            connString += " -h " + dbHost
        if dbPort != None and dbPort != '':
            connString += " -p " + dbPort
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

def size(outputFolder, format, connString):
    try:
        size_indexes = float(((os.popen("stat -Lc %s " + outputFolder + "/*.lax | awk '{t+=$1}END{print t}'")).read().split('\t'))[0]) / (1024. * 1024.)
    except:
        size_indexes = 0.
    if connString != None:
        connection = psycopg2.connect(connString)
        cursor = connection.cursor()
        cursor.execute("""SELECT sum(pg_indexes_size(tablename::text)) / (1024*1024) size_indexes,  sum(pg_table_size(tablename::text)) / (1024*1024) size_ex_indexes, sum(pg_total_relation_size(tablename::text)) / (1024*1024) size_total FROM pg_tables where schemaname='public'""")
        size_indexes += float(list(cursor.fetchone())[-1])
        connection.close()
    try:
        size_ex_indexes = float(((os.popen("stat -Lc %s " + outputFolder + "/*." +  format + " | awk '{t+=$1}END{print t}'")).read().split('\t'))[0]) / (1024. * 1024.)
    except:
        size_ex_indexes = 0
    size_total = size_indexes + size_ex_indexes
    return 'Size indexes= ' +  ('%.3f MB' % size_indexes) + '. Size excluding indexes= ' +  ('%.3f MB' % size_ex_indexes) + '. Size total= ' +  ('%.3f MB' % size_total)

def getNumPoints(outputFolder, format, connString, tableName, bindir) :
    try:
        if connString != None:
            n = int(os.popen('psql ' + connString + ' -c "select sum(num) from ' + tableName + '" -t -A').read())
        else:
            n = int(getLASParams(outputFolder + "/*." +  format, bindir)[0])
    except: 
        n = 0
    return 'Number of points: ' + str(n)

def processMulti(inputItems, bindir, numProcessesLoad, outputFolder, sort, format, connString, tableName, srid):
    """ Process the input items with a multiple cores/processes"""
    numItems = len(inputItems)
    # We create a tasts queue and fill it with as many tasks as input files
    childrenQueue = multiprocessing.Queue()
    for i in range(numItems):
        inputItem = inputItems[i]
        childrenQueue.put([i, inputItem])
    for i in range(numProcessesLoad): #we add as many None jobs as numProcessesLoad to tell them to terminate (queue is FIFO)
        childrenQueue.put(None)
        
    resultQueue = multiprocessing.Queue() # The queue where to put the results
    children = []
    # We start numProcessesLoad children processes
    for i in range(numProcessesLoad):
        children.append(multiprocessing.Process(target=runChildLoad,
            args=(i, childrenQueue, resultQueue, bindir, outputFolder, sort, format, connString, tableName, srid)))
        children[-1].start()
    
    # Collect the results
    results = []         
    for i in range(numItems):        
        [identifier, inputItem, t] = resultQueue.get()
        logging.info('load from ' + inputItem + ' finished in ' + ('%.2f' % t) + ' seconds. ' + str(i+1) + '/' + str(numItems) + ' (' + ('%.2f' % (float(i+1) * 100. / float(numItems))) + '%)')
        results.append([inputItem, t])
    
    # wait for all children to finish their execution
    for i in range(numProcessesLoad):
        children[i].join()
        
    return results

def runChildLoad(childIndex, childrenQueue, resultQueue, bindir, outputFolder, sort, format, connString, tableName, srid):
    kill_received = False
    if connString != None:
        connection = psycopg2.connect(connString)
        cursor = connection.cursor()
    while not kill_received:
        job = None
        try:
            # This call will patiently wait until new job is available
            job = childrenQueue.get()
        except:
            # if there is an error we will quit the loop
            kill_received = True
        if job == None:
            # If we receive a None job, it means we can stop the grandchildren too
            kill_received = True
        else:            
            [index, fileAbsPath] = job
            ti = time.time()
            try:
                iFormat = fileAbsPath.split('.')[-1]
                if iFormat == format:
                    outputAbsPath = outputFolder + '/' + os.path.basename(fileAbsPath)
                else:
                    outputAbsPath = outputFolder + '/' + os.path.basename(fileAbsPath).replace(iFormat, format)
                commands = []
                if sort:
                    commands.append(bindir + '/lassort.exe -i ' + fileAbsPath + ' -o ' + outputAbsPath)
                else:
                    if iFormat == format:
                        commands.append('ln -s ' + fileAbsPath + ' ' + outputAbsPath)
                    else:
                        commands.append(bindir + '/las2las -i ' + fileAbsPath + ' -o ' + outputAbsPath)
                commands.append(bindir + '/lasindex -i ' + outputAbsPath)
                
                for command in commands:
                    logging.info(command)
                    os.system(command)
                    
                if connString != None:
                    (count, minX, minY, minZ, maxX, maxY, maxZ, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ) = getLASParams(fileAbsPath, bindir)
                    insertStatement = """INSERT INTO """ + tableName + """(id,filepath,num,scalex,scaley,scalez,offsetx,offsety,offsetz,geom) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_MakeEnvelope(%s, %s, %s, %s, %s));"""
                    insertArgs = [index, fileAbsPath, int(count), float(scaleX), float(scaleY), float(scaleZ), float(offsetX), float(offsetY), float(offsetZ), float(minX), float(minY), float(maxX), float(maxY), srid]
                    logging.info(cursor.mogrify(insertStatement, insertArgs))
                    cursor.execute(insertStatement, insertArgs)
                    connection.commit()
                    
            except Exception,e:
                logging.exception('ERROR loading from ' + fileAbsPath + ':\n' + str(e))
            resultQueue.put([index, fileAbsPath, time.time() - ti])
    if connString != None:
        connection.close()

def main(opts):
    # Set logging 
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt=TIMEFORMAT, level=logging.DEBUG)
    
    # Check the options
    if opts.input == '' or not os.path.exists(opts.input):
        logging.error('You must specify a existing input folder')
        return
    if opts.bindir == '' or not os.path.exists(opts.bindir):
        logging.error('You must specify a a valid LASTools binary directory')
        return
    binaries = os.listdir(opts.bindir)
    for tool in ('lasinfo', 'lasclip.exe', 'lasmerge', 'las2las', 'lasindex', 'lassort.exe'):
        if tool not in binaries:
            logging.error(tool + ' is not found in ' + opts.bindir)
            return    
    if opts.output == '':
        logging.error('You must specify a output folder')
        return
    if opts.format not in ('las', 'laz'):
        logging.error('Format for the DMS must be las or laz')
        return
    try:
        numproc = int(opts.numproc)
    except:
        logging.error('Specify a valid number of processes')
        return
    if opts.srid == '':
        logging.error('Specify the SRID of your data')
        return
    try:
        srid = int(opts.srid)
    except:
        logging.error('Specify a valid SRID (integer)')
        return
    
    # Check the DB details, if DB is used we create the DB and the table
    connString = None
    clineConnString = None
    connection = None
    cursor = None
    if not opts.nodb:
        if opts.db == '':
            logging.error('None specified DB connection details ')
            return
        (dbuser,dbpass,dbhost,dbport,dbname) = opts.db.split(':')
        connString = postgresConnectString(dbname, dbuser, dbpass, dbhost, dbport, False)
        clineConnString = postgresConnectString(dbname, dbuser, dbpass, dbhost, dbport, True)
    
        os.system('dropdb ' + clineConnString)
        os.system('createdb ' + clineConnString)
    
        connection = psycopg2.connect(connString)
        cursor = connection.cursor()
        cursor.execute('CREATE EXTENSION postgis')
        connection.commit()
        q = """
        CREATE TABLE """ + opts.table + """ (
            id integer,
            filepath text,
            num integer,
            scalex double precision,
            scaley double precision,
            scalez double precision,
            offsetx double precision,     
            offsety double precision,
            offsetz double precision,
            geom public.geometry(Geometry,""" + opts.srid + """)
        )"""
        logging.info(cursor.mogrify(q))
        cursor.execute(q)
        connection.commit()
    
    # Remove output directory 
    os.system('rm -rf ' + opts.output)
    # Create output directory
    os.system('mkdir -p ' + opts.output)

    # get the list of input files (this gets files recursively in subfolders as well)
    inputFiles = [os.path.join(dirpath, f) 
    for dirpath, dirnames, files in os.walk(opts.input)
    for f in files if f.endswith('.las') or f.endswith('.laz')]
    
    if len(inputFiles) == 0:
        raise Exception('No las/laz files found in ' + opts.input)

    # Start the processing
    t0 = time.time()
    
    # We execute the loading in parallel
    processMulti(inputFiles, opts.bindir, numproc, opts.output, opts.sort, opts.format, connString, opts.table, srid)
    
    if connString != None:
        # If DB is used we need to create the index data
        q = "create index ON " + opts.table + " using GIST (geom)"
        logging.info(cursor.mogrify(q))
        cursor.execute(q)
        connection.commit()
        # We also vacuum and analyze the DB table
        old_isolation_level = connection.isolation_level
        connection.set_isolation_level(0)
        q = "VACUUM FULL ANALYZE " + opts.table
        logging.info(cursor.mogrify(q))
        cursor.execute(q)
        connection.commit()
        connection.set_isolation_level(old_isolation_level)
        cursor.close() 
        connection.close()    
    
    # Get information on the number of points, the size
    logging.info(getNumPoints(opts.output, opts.format, clineConnString, opts.table, opts.bindir))
    logging.info(size(opts.output, opts.format, connString))
    logging.info('Total time is: %.2f seconds' % (time.time() - t0))
        
        
if __name__ == "__main__":
    usage = 'Usage: %prog [options]'
    description = "Prepares the data for LASTools based point cloud data management system"
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--input',default='',help='The folder with all the input files. Any subfolders structure will not be kept in the output data. IMPORTANT: If there are subfolders please be sure there are not files with the same name',type='string')
    op.add_option('-l','--bindir',default='',help='LASTools bin directory', type='string')
    op.add_option('-s','--sort',help='Uses lassort to resort the points',default=False,action='store_true')
    op.add_option('-n','--nodb',help='Do not use DB to store extent indexes. This will slow down the queries. [default False]',default=False,action='store_true')
    op.add_option('-d','--db',default='',help='Connection string in the form of ' + DB_DETAILS_FORMAT + '. IMPORTANT: Any previous database with same name will be deleted', type='string')
    op.add_option('-t','--table',help='Table name to be used in the DB [default ' + DEFAULT_TABLE_NAME + ']', type='string', default=DEFAULT_TABLE_NAME)
    op.add_option('-r','--srid',default='',help='SRID of the input data', type='string')
    op.add_option('-o','--output',default='',help='The folder that will contain the indexed (and ressorted if sort is selected) data. IMPORTANT: Any data in this folder will be deleted!', type='string')
    op.add_option('-f','--format',default='',help='Format to be used (las or laz). LAS format requires around 10 times more storage but will be faster to index and resort as well as when querying', type='string')
    op.add_option('-p','--numproc',help='Number of processes to be used [default 1]', type='string', default='1')
    (opts, args) = op.parse_args()
    main(opts)
