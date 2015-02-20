#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from pointcloud.quadtree import QuadTree as QuadTree

def getQuadTree(configuration, minX, minY, maxX, maxY, mortonScaleX, mortonScaleY, globalOffsetX = 0, globalOffsetY = 0):
        mortonDistinctIn = configuration.getboolean('Query','MortonDistinctIn')
        numLevels = configuration.get('Query','MortonQuadTreeNumLevels')
        if numLevels != 'auto':
            numLevels = int(numLevels)
        mortonApprox = configuration.getboolean('Query','MortonApproximation')
        domain = (int((minX-globalOffsetX)/mortonScaleX), int((minY-globalOffsetY)/mortonScaleY), int((maxX-globalOffsetX)/mortonScaleX), int((maxY-globalOffsetY)/mortonScaleY))
        maxRanges = configuration.getint('Query','MortonMaxRanges')
        if maxRanges == -1:
            maxRanges = None
        return (QuadTree(domain, numLevels), mortonDistinctIn, mortonApprox, maxRanges)
