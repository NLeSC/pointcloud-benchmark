import argparse, traceback, time, os, math, psycopg2
#from pointcloud import lasops, postgresops

def argument_parser():
    """ Define the arguments and return the parser object"""
    parser = argparse.ArgumentParser(
    description="Create a folder structure with the data spatially sorted in XY tiles")
    parser.add_argument('-i','--input',default='',help='Input data folder (with LAS/LAZ files)',type=str, required=True)
    parser.add_argument('-s','--srid',default='',help='SRID of the input data',type=int, required=True)
    parser.add_argument('-o','--output',default='',help='Output data folder for the different tiles',type=str, required=True)
    parser.add_argument('-n','--number',default='',help='Number of tiles',type=int, required=True)
    parser.add_argument('-d','--dbname',default='',help='PostgreSQL DB name',type=str, required=True)
    parser.add_argument('-u','--dbuser',default='',help='DB user',type=str, required=True)
    parser.add_argument('-p','--dbpass',default='',help='DB pass',type=str, required=True)
    parser.add_argument('-b','--dbhost',default='',help='DB host',type=str, required=True)
    parser.add_argument('-r','--dbport',default=5432,help='DB port [default 5432]',type=int)
    return parser

def run(inputFolder, srid, outputFolder, numberTiles, dbName, dbUser, dbPass, dbHost, dbPort):
    
    # Check input parameters
    if not os.path.isdir(inputFolder) and not os.path.isfile(inputFolder):
        raise Exception('Error: Input folder does not exist!')
    
    if os.path.isfile(outputFolder):
        raise Exception('Error: There is a file with the same name as the output folder. Please, delete it!')
    elif os.path.isdir(outputFolder) and os.listdir(outputFolder):
        raise Exception('Error: Output folder exists and it is not empty. Please, delete the data in the output folder!')
    
    axisTiles = math.sqrt(numberTiles) 
    if not axisTiles.is_integer():
        raise Exception('Error: Number of tiles must be a square number!')
    axisTiles = int(axisTiles)
    
    # Create output folder
    os.system('mkdir -p ' + outputFolder)
    
    connection = psycopg2.connect(postgresops.getConnectString(dbName, dbUser, dbPass, dbHost, dbPort))
    cursor = connection.cursor()
    
#    (inputFiles, _, numberPoints, minX, minY, _, maxX, maxY, _, scaleX, scaleY, _) = lasops.getPCFolderDetails(inputFolder)
    (inputFiles, numberPoints, minX, minY, maxX, maxY, scaleX, scaleY) = ([], 640000000000, 13427.64, 306746.26, 278487.77, 615431.24, 0.01, 0.01)
    
    print '%s contains %d files with %d points. The XY extent is %.2f, %.2f, %.2f, %.2f' % (inputFolder, len(inputFiles), numberPoints, minX, minY, maxX, maxY)
    
    rangeX = maxX - minX 
    rangeY = maxY - minY
    
    tileSizeX = rangeX / float(axisTiles)
    tileSizeY = rangeY / float(axisTiles)
    
    tilesTableName = 'TILES'
    tileCounter = 0
    
    cursor.execute("""
    CREATE TABLE """ + tilesTableName + """ (
        id integer,
        geom public.geometry(Geometry,""" + str(srid) + """)
    )""")
    
    for xIndex in range(axisTiles):
        for yIndex in range(axisTiles):
            minTileX = minX + (xIndex * tileSizeX)
            maxTileX = minX + ((xIndex+1) * tileSizeX)
            minTileY = minY + (yIndex * tileSizeY)
            maxTileY = minY + ((yIndex+1) * tileSizeY)
            # To avoid overlapping tiles
            if xIndex < axisTiles-1:
                maxTileX -= scaleX
            if yIndex < axisTiles-1:
                maxTileY -= scaleY
                
            print '\t'.join((str(xIndex), str(yIndex), '%.2f' % minTileX, '%.2f' % minTileY, '%.2f' % maxTileX, '%.2f' % maxTileY))
            insertStatement = "INSERT INTO " + tilesTableName + """(id,geom) VALUES (%s, ST_MakeEnvelope(%s, %s, %s, %s, %s));"""
            insertArgs = [tileCounter, minTileX, minTileY, maxTileX, maxTileY, srid]
            cursor.execute(insertStatement, insertArgs)
    
            tileCounter += 1
            
    for inputFile in inputFiles:
        (_, _, minX, minY, _, maxX, maxY, _, _, _, _, _, _, _) = lasops.getPCFileDetails(absPath)
        # Check how many tiles it overlaps
        # if one, we just copy it to the tile folder
        # if many we run lasmerge top create the different parts!
        
            
if __name__ == "__main__":
    args = argument_parser().parse_args()
    print 'Input folder: ', args.input
    print 'SRID: ', args.srid  
    print 'Output folder: ', args.output
    print 'Number of tiles: ', args.number
    print 'DB name: ', args.dbname
    print 'DB user: ', args.dbuser
    print 'DB pass: ', '*' * len(args.dbpass)
    print 'DB host: ', args.dbhost
    print 'DB port: ', args.dbport
    
    try:
        t0 = time.time()
        print 'Starting ' + os.path.basename(__file__) + '...'
        run(args.input, args.srid, args.output, args.number, args.dbname, args.dbuser, args.dbpass, args.dbhost, args.dbport)
        print 'Finished in %.2f seconds' % (time.time() - t0)
    except:
        print 'Execution failed!'
        print traceback.format_exc()