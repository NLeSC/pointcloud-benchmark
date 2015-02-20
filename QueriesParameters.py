#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
from lxml import etree as ET
import os, glob
from pointcloud import wktops
from pointcloud.QueryParameters import QueryParameters

class QueriesParameters:
    """Abstract class for the query parameters"""
    def __init__(self, absPath):
        """ Create the query parameters instance by providing the XML path"""
        if os.path.isdir(absPath):
            self.data = None
            for xml_file in glob.glob(absPath +"/*.xml"):
                filedata = ET.parse(xml_file).getroot()
                if self.data is None:
                    self.data = filedata 
                else:
                    self.data.extend(filedata) 
        elif os.path.isfile(absPath):
            self.data = ET.parse(absPath).getroot()
        else:
            raise Exception(absPath + ' not found')
    
    def getIds(self):
        """ Get query indexes """
        return sorted(self.data.xpath('/queries/query/@id'))   
    
    def getQuery(self, index):
        """ Get the query element """
        return self.data.xpath('/queries/query[@id = "' + str(index) + '"]')[0]
    
    def getType(self, query):
        """ Get query type """
        return query.find('type').text
    
    def getKey(self, query):
        """ Get query key, i.e. description or name of the query without white spaces"""
        return query.get('key')
    
    def getWKT(self, query):
        """ Get WKT of the query"""
        return query.find('wkt').text.replace('\n','').strip()
    
    def getBuffer(self, query):
        """ Get Buffer, this will only be available in polyline+buffer query type"""
        return float(query.find('buffer').text)
    
    def getMinZ(self, query):
        """ Get min z, this will only be available in rectangle+z"""
        m = query.find('minz')
        rm = None
        if m.text != None:
            rm = float(m.text)
        return rm
    
    def getMaxZ(self, query):
        """ Get Buffer, this will only be available in rectangle+z"""
        m = query.find('maxz')
        rm = None
        if m.text != None:
            rm = float(m.text)
        return rm
    
    def getNum(self, query):
        """ Get number, this will only be available in nn (nearest neighbour)"""
        return int(query.find('num').text)
    
    def getRadius(self, query):
        """ Get Radius, this will only be available in nn (nearest neighbour)"""
        return int(query.find('radius').text)
    
    def getStatistics(self, query):
        """ Get Statistic"""
        return query.find('stat').text.split(',')
    
    def getColumns(self, query):
        """ Get columns"""
        return query.find('columns').text
    
    def getQueryParameters(self, db, queryId, validColumns):
        query = self.getQuery(queryId)
        
        queryKey = self.getKey(query)
        queryType = self.getType(query)
        wkt = self.getWKT(query)
        
        columns = self.getColumns(query)
        for column in columns:
            if column not in validColumns:
                raise Exception('Error: column ' + column)
        
        (minx,maxx,miny,maxy) = (None,None,None,None)
        (cx,cy,rad) = (None,None,None)
        (minz,maxz) = (None, None)
        (px,py,nnnum,nnrad) = (None,None,None,None)
        statistics = None
        
        if queryType.endswith('+z'):
            queryType = queryType.replace('+z', '')
            minz = self.getMinZ(query)
            maxz = self.getMaxZ(query)
        
        if queryType.endswith('+stat'):
            queryType = queryType.replace('+stat', '')
            statistics = self.getStatistics(query)
            if len(statistics) != len(columns):
                raise Exception('Error in query parameters: columns and statistics must have same length')
        
        if queryType != 'nn':
            (mins, maxs) = wktops.wktPolygon2MinMax(wkt)
            (minx,maxx,miny,maxy) = (mins[0],maxs[0],mins[1],maxs[1])
            if queryType == 'circle' :
                (center, rad) = wktops.wktPolygonToCenterRad(wkt)
                (cx,cy,rad) = center[0], center[1], rad
        else:
            (px, py) = wktops.wktPointToCoordinates(wkt)
            nnnum = self.getNum(query)
            nnrad = self.getRadius(query)
            
        return QueryParameters(db,queryKey,queryType,columns,statistics,minx,maxx,miny,maxy,cx,cy,rad,minz,maxz,px,py,nnnum,nnrad)
