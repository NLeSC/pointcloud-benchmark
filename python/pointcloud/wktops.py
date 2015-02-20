#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################

def wktPolygon2MinMax(wkt):
    wkt = wkt.lower().replace('polygon','').replace('linestring','').replace('(','').replace(')','').replace('"','').strip()
    mins = None
    maxs = None
    for p in wkt.split(','):
        coordinates = p.strip().split(' ')
        if mins == None:
            mins = []
            maxs = []
            for j in range(len(coordinates)):
                mins.append(None)
                maxs.append(None)
        for i in range(len(coordinates)): 
            coordinates[i] = float(coordinates[i])
            if (mins[i] == None) or (coordinates[i] < mins[i]):
                mins[i] = coordinates[i]
            if (maxs[i] == None) or (coordinates[i] > maxs[i]):
                maxs[i] = coordinates[i]
    return (mins, maxs)

def wktPolygonToCenterRad(wkt):
    (mins, maxs) = wktPolygon2MinMax(wkt)
    center = []
    for i in range(len(mins)):
        center.append(mins[i] + ((maxs[i] - mins[i]) / 2.))
    return (center, (maxs[0]-mins[0]) / 2.)

def scale(wkt, scaleX, scaleY, offsetX = 0., offsetY = 0.):
    num = ''
    output = ''
    i = 0
    for c in wkt:
        if c.isdigit() or c == '.':
            num += c 
        else:
            if num != '':
                if i % 2:
                    scale = scaleY
                    offset = offsetY
                else:
                    scale = scaleX
                    offset = offsetX
                i+=1
                output += str(int((float(num)-offset) / scale))
                num = ''
            output += c
    return output

def wktPointToCoordinates(wkt):
    if wkt.count('POINT') == 0:
        raise Exception('ERROR: WKT does not represent a POINT') 
    [x,y] = wkt.upper().replace('POINT','').replace('(','').replace(')','').split()
    return (float(x),float(y))
