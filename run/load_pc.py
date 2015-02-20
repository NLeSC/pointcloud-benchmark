#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, optparse, sys
from ConfigParser import ConfigParser
from pointcloud import utils
import logging

def main(opts):
    config = ConfigParser()
    config.optionxform=str
    if opts.ini == '':
        print 'ERROR: You must specify a valid ini file path. Use -h for help'
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
        loadermodule = config.get('General','Loader')
        loadername = loadermodule.split('.')[-1]
        
        exec 'from ' + loadermodule + ' import ' + loadername
        loader = getattr(sys.modules[loadermodule], loadername)(config)
        # Loads data into DB
        loader.run()
        
if __name__ == "__main__":
    usage = 'Usage: %prog [options]'
    description = "Run the loading procedure"
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--ini',default='',help='Configuration file describing the parameters for the loading',type='string')
    op.add_option('-o','--options',default='',help='Options to override .ini file [optional]. If you specify an option using this method the one in the .ini file is ignored. Use [section]:[option]:[value]. If multiple, specify them blank space separated. You will have to use "" in such case. Example -o "General:ExecutionPath:test DB:Host:mydb"',type='string')
    op.add_option('-s','--show',help='Show options and exit? [default False]',default=False,action='store_true')
    (opts, args) = op.parse_args()
    main(opts)
