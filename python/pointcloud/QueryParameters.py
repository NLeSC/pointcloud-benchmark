#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################

class QueryParameters:
    """Query parameters"""
    def __init__(self,db,queryKey=None,queryMethod=None,queryType=None,wkt=None,columns=None,statistics=None,minx=None,maxx=None,miny=None,maxy=None,cx=None,cy=None,rad=None,minz=None,maxz=None,px=None,py=None,nnnum=None,nnrad=None):    
        (self.queryKey, self.queryMethod, self.queryType) = (queryKey, queryMethod, queryType)
        self.wkt = wkt
        (self.columns, self.statistics) = (columns, statistics)
        (self.minx,self.maxx,self.miny,self.maxy) = (minx,maxx,miny,maxy)
        (self.minz,self.maxz) = (minz,maxz)
        if queryType != 'nn':
            (self.cx,self.cy,self.rad) = (cx,cy,rad)
        else:
            (self.cx,self.cy,self.rad,self.num) = (px,py,nnrad,nnnum)
        
        self.db = db
        if db == 'mon':
            self.pattern = '%s'
            self.hardcode = False
            self.powermethod = True
        elif db == 'psql':
            self.pattern = '%s'
            self.hardcode = False
            self.powermethod = False
        elif db == 'ora':
            self.pattern = ''
            self.hardcode = True
            self.powermethod = True
        else:
            raise Exception('Unknown DB!')    
            
