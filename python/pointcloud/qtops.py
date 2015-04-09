#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from pointcloud.quadtree import QuadTree as QuadTree

def getQuadTree(minX, minY, maxX, maxY, mortonScaleX, mortonScaleY, globalOffsetX = 0, globalOffsetY = 0):
        mortonDistinctIn = False
        numLevels = 'auto'
        if numLevels != 'auto':
            numLevels = int(numLevels)
        mortonApprox = False
        domain = (int((minX-globalOffsetX)/mortonScaleX), int((minY-globalOffsetY)/mortonScaleY), int((maxX-globalOffsetX)/mortonScaleX), int((maxY-globalOffsetY)/mortonScaleY))
        maxRanges = -1
        if maxRanges == -1:
            maxRanges = None
        return (QuadTree(domain, numLevels), mortonDistinctIn, mortonApprox, maxRanges)
