#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import sys
from pointcloud import utils

inputFile = sys.argv[1]
outputFile = sys.argv[2]

(times, rdata, wdata) = utils.parseIO(inputFile)

if len(sys.argv) != 3:
    step = int(sys.argv[3])
    times = times[::step]
    for k in rdata:
        rdata[k] = utils.chunkedMean(rdata[k],step)
        wdata[k] = utils.chunkedMean(wdata[k],step)
        
utils.saveIO(times, rdata, wdata, 'IO', outputFile)
