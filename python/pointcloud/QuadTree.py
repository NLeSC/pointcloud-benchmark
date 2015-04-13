#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import shapely, logging, numpy, math
from shapely.wkt import loads
from shapely.geometry import box
from de9im.patterns import intersects, contains, pattern
excluding_interiors = pattern('F********')
from pointcloud.morton import morton

# maximum number of bits for X and Y
MAX_NUMBITS = 31
    
class QuadTree:
    """ QuadTree class """
    def __init__(self, domain, numLevels):
        maximumValue = max(domain)
        minimumValue = min(domain)
        if minimumValue < 0:
            raise Exception('ERROR: Domain must contain only positive X and Y numbers!')
        self.numBits = MAX_NUMBITS
        fits = True
        while fits:
            if (1 << self.numBits) >= maximumValue:
                self.numBits -= 1
            else:
                fits = False
                self.numBits += 1
        if self.numBits > MAX_NUMBITS:
            raise Exception('ERROR: maximum number of bits of X and Y is ' + str(MAX_NUMBITS))
        
        if numLevels != 'auto' and numLevels > 0:
            if numLevels > self.numBits:
                raise Exception('ERROR: quadTree numLevels must be lower or equal to the number of bits of X and Y')
            else:
                self.numLevels = numLevels
        else:
            self.numLevels = 'auto'     
        
        mindomain = 0
        maxdomain = 1 << self.numBits
        parentQuad = (mindomain, mindomain, maxdomain, maxdomain)
        startLevel = 0
        self.domainRegion = box(*domain)
        fits = True
        while fits:
            numCodes = len(self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentQuad)[0])
            if numCodes == 1:
                startLevel += 1
            else:
                fits = False
                startLevel -= 1
        
        if startLevel > 0:
            (self.startQuadCode, self.startLevel, startFullIn, startMRange) = self._overlapCodes(startLevel, 0, 0, self.domainRegion, *parentQuad)[0][0]
            self.startQuad = self.getCoords(startMRange)
        else:
            self.startLevel = 0
            self.startQuadCode = 0
            self.startQuad = parentQuad
        
#         print 'domain', domain
#         print 'domain numBits', self.numBits
#         print 'quadtree numLevels', self.numLevels
#         print 'quadtree startLevel', self.startLevel
#         print 'quadtree startQuadCode', self.startQuadCode
#         print 'quadtree startQuad', self.startQuad
        
    def _relation(self, geom1, geom2):
        """ Returns the relationship between two geometries. 
              0 if they are disjoint, 
              1 if geom2 is completely in geom1,
              2 if geom2 is partly in geom1"""
        relation = geom1.relate(geom2)
        
        if not intersects.matches(relation):
            return 0 # they are disjoint
        elif contains.matches(relation):
            return 1 
        else: # there is some overlaps
            if excluding_interiors.matches(relation):
                return 0 # overlap only in boundaries, we do not count it
            else:
                return 2 # some interior of geom2 is in geom1
    
    def _overlapCodes(self, maxDepth, parentLevel, parentCode, region, minx, miny, maxx, maxy): 
        """ Recursive method that return morton ranges overlapping with the region for the specified domain"""
        cx = minx + ((maxx - minx) >> 1)
        cy = miny + ((maxy - miny) >> 1)
          
        quads = [
           (minx, miny, cx, cy), #0
           (minx, cy, cx, maxy), #1
           (cx, miny, maxx, cy), #2
           (cx, cy, maxx, maxy)  #3
        ]
        
        level = parentLevel + 1
          
        codes = []
        c = 0
        for quadIndex in range(4):
            quad = quads[quadIndex]
            relation = self._relation(region, box(*quad))
            if relation: #1 or 2
                quadCode = (parentCode << 2) + quadIndex
                if relation == 1 or parentLevel == maxDepth:
                    codes.append((quadCode, level, relation == 1, self.quadCodeToMortonRange(quadCode, level))) # relation = 1 indicates that this morton range is fully withoin query region
                    c += 1
                else:
                    (tcodes, tc) = self._overlapCodes(maxDepth, level, quadCode, region, *quad)
                    if tc == 4:
                        codes.append((quadCode, level, False, self.quadCodeToMortonRange(quadCode, level)))
                        c += 1
                    else:
                        codes.extend(tcodes)
        return (codes,c)
    
    
    def quadCodeToMortonRange(self, quadCode, level):
        diff = (self.numBits - level) << 1
        minr = quadCode << diff
        maxr = ((quadCode+1) << diff) - 1
        return (minr,maxr)
        
    def overlapCodes(self, region, numLevels = None):
        if numLevels == None:
            numLevels = self.numLevels
        if (numLevels == 'auto') or (numLevels < 0):
            numLevels = int(math.ceil(math.log(self.domainRegion.area / region.area,2) / 2.)) + 0
        #print 'Py ', numLevels, self.domainRegion.area, region.area
        if box(*self.startQuad).intersects(region):
            return self._overlapCodes(numLevels, self.startLevel, self.startQuadCode, region, *self.startQuad)[0]
        return []
    
    def mergeConsecutiveRanges(self, mranges):
        if len(mranges) == 0:
            return []
        omranges = []
        (mrangemin, mrangemax) = mranges[0]
        for rangeIndex in range(1, len(mranges)):
            mrange = mranges[rangeIndex]
            if mrangemax == mrange[0] - 1:
                mrangemax = mrange[1]
            else:
                omranges.append((mrangemin, mrangemax))
                (mrangemin, mrangemax) = mrange
        omranges.append((mrangemin, mrangemax))
        return omranges
    
    def mergeRanges(self, mranges, maxRanges):
        numRanges = len(mranges)
        if numRanges <= maxRanges or numRanges < 2:
            return mranges 
        numRangesToMerge = numRanges - maxRanges
        b = numpy.array(numpy.array(mranges).flat)
        diffs = b[::2][1:] - b[1::2][:-1]
        tDiff = sorted(diffs)[numRangesToMerge-1]
        lowerDiffs = len(diffs[diffs < tDiff])
        equalToMerge = numRangesToMerge - lowerDiffs
        equalCounter = 0
        omranges = []
        mrangemin = None    
        for rangeIndex in range(numRanges):
            if mrangemin == None:
                mrangemin = mranges[rangeIndex][0]
            if rangeIndex < numRanges-1:
                if diffs[rangeIndex] > tDiff:
                    omranges.append((mrangemin, mranges[rangeIndex][1]))
                    mrangemin = None
                elif diffs[rangeIndex] == tDiff:
                    equalCounter += 1
                    if equalCounter > equalToMerge:
                        omranges.append((mrangemin, mranges[rangeIndex][1]))
                        mrangemin = None
            else:
                omranges.append((mrangemin, mranges[rangeIndex][1]))
        return omranges
            
    def getAllRanges(self, codes):
        mranges = []
        for code in codes:
            mranges.append(code[-1])
        return mranges
    
    def getDiffRanges(self, codes):
        imranges = []
        omranges = []
        for code in codes:
            if code[2]:
                imranges.append(code[-1])
            else:
                omranges.append(code[-1])
        return (imranges, omranges)
    
    def getCoords(self, mortonRange):
        (minr, maxr) = mortonRange
        minx = morton.DecodeMorton2DX(minr)
        miny = morton.DecodeMorton2DY(minr)
        maxx = morton.DecodeMorton2DX(maxr)
        maxy = morton.DecodeMorton2DY(maxr)
        return (minx,miny,maxx+1,maxy+1)
    
    def getMortonRanges(self, wkt, distinctIn = False, numLevels = None, maxRanges = None):
        codes = self.overlapCodes(loads(wkt), numLevels)
        if distinctIn:
            (imranges, xmranges) = self.getDiffRanges(codes)
            mimranges = self.mergeConsecutiveRanges(imranges)
            mxmranges = self.mergeConsecutiveRanges(xmranges)
            logging.debug(' '.join((' #mranges:' , str(len(codes)), ' #imranges:' , str(len(imranges)), ' #xmranges:' , str(len(xmranges)), ' #mimranges:' , str(len(mimranges)), ' #mxmranges:' , str(len(mxmranges)))))
            return (mimranges, mxmranges)
        else:
            mmranges = self.mergeConsecutiveRanges(self.getAllRanges(codes))
            if maxRanges != None:
                maxmranges = self.mergeRanges(mmranges, maxRanges)
                logging.debug('#mranges:' + str(len(codes)) + ' #mmranges:' + str(len(mmranges)) + ' #maxmranges:' + str(len(maxmranges)))
                return ([], maxmranges)
            else:
                logging.debug('#mranges:' + str(len(codes)) + ' #mmranges:' + str(len(mmranges)))
                return ([], mmranges)
    
    def mortonToQuadCell(self, morton, level):
        return (((1 << 2*level) - 1) << ((2 * MAX_NUMBITS) - 2*level)) & morton
