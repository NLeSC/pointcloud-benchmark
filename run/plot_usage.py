#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import sys
from pointcloud import utils

inputFile = sys.argv[1]
outputFile = sys.argv[2]

(times, cpus, mems) = utils.parseUsage(inputFile)

if len(sys.argv) != 3:
    step = int(sys.argv[3])
    times = times[::step]
    cpus = utils.chunkedMean(cpus, step)
    mems = utils.chunkedMean(mems, step)

utils.saveUsage(times, cpus, mems, "CPU / MEM", outputFile)
