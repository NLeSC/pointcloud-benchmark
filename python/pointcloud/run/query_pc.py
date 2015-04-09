#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, optparse, sys
from ConfigParser import ConfigParser
from pointcloud import utils
from pointcloud.tabulate import tabulate
import logging

def main(opts):
    config = ConfigParser()
    config.optionxform=str
    if opts.ini == '':
        print 'ERROR: You must specify a valid ini file path.'
        return
    if not os.path.isfile(opts.ini):
        print 'ERROR: Specified ini file is not found' 
        return 

    config.read(opts.ini)
    utils.setOptions(opts.options, config)
    print utils.showOptions(config, ['General', 'DB', 'Load', 'Query'])
        
    # Set logging 
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt=utils.DEFAULT_TIMEFORMAT, level=getattr(logging, config.get('General','LogLevel')))
    
    if not opts.show:
        queriermodule = config.get('General','Querier')
        queriername = queriermodule.split('.')[-1]
        
        exec 'from ' + queriermodule + ' import ' + queriername
        querier = getattr(sys.modules[queriermodule], queriername)(config)

        # Run the queries
        stats = querier.run()
        
        h = ('#Query','Points','Waste','Time','CPU','MEM')
        rows = []
        for stat in stats:
            (queryName, qTime, qResult, qCPU, qMEM) = stat
            if qTime == None or qTime == '-':
                qTime = '-'
            else:
                qTime = '%.2f'%qTime
            if qCPU == None or qCPU == '-':
                qCPU = '-'
                qMEM = '-'
            else:
                qCPU = '%.2f'%qCPU
                qMEM = '%.2f'%qMEM
            rows.append((str(queryName),str(qResult),'-',qTime,qCPU,qMEM))        
        
        outputFile = open(utils.RESULTS_FILE_NAME,'w')
        outputFile.write(tabulate(rows, headers=h) + '\n')
        outputFile.close()
        logging.info('Check results in ' + os.path.abspath(utils.RESULTS_FILE_NAME))

if __name__ == "__main__":
    usage = 'Usage: %prog [options]'
    description = "Run the querier"
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--ini',default='',help='Configuration file describing the parameters for the queries execution',type='string')
    op.add_option('-o','--options',default='',help='Options to override .ini file [optional]. If you specify an option using this method the one in the .ini file is ignored. Use [section]:[option]:[value]. If multiple, specify them blank space separated. You will have to use "" in such case. Example -o "General:ExecutionPath:test DB:Host:mydb"',type='string')
    op.add_option('-s','--show',help='Show options and exit? [default False]',default=False,action='store_true')
    (opts, args) = op.parse_args()
    main(opts)
