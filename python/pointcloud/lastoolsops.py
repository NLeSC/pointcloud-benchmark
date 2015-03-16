#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os
import utils

#
# This module contains methods that use LAStools
#

# Check the LAStools is installed and that it is in PATH before libLAS
if utils.shellExecute('lasinfo -version').count('LAStools') == 0:
    raise Exception("LAStools is not found!. Please check that it is in PATH and that it is before libLAS")

def getNumPoints(outputFile):
    npointscommand = "lasinfo " + outputFile+ " -nc -nv -nco 2>&1 | grep 'number of point records:'"
    try:
        result  = int(utils.shellExecute(npointscommand).split()[-1])
    except:
        result = None
        
