#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import argparse, random
from shapely.affinity import translate
from shapely.wkt import loads, dumps
from de9im.patterns import contains, intersects

def main(opts):
    pattern = loads(open(opts.input, 'r').read())
    extent = loads(open(opts.extent, 'r').read())
    
    if not contains.matches(extent.relate(pattern)):
        print 'ERROR: pattern must be contained within the extent'
        return
    
    c = pattern.centroid
    (xs , ys) = extent.boundary.xy
    (minx, maxx, miny, maxy) = (min(xs) - c.x, max(xs) - c.x, min(ys) - c.y, max(ys) - c.y)
    
    
    outputFile = open(opts.output, 'w')
    
    geoms = []
    
    while len(geoms) < opts.number:
        dx = random.uniform(minx, maxx) 
        dy = random.uniform(miny, maxy)
        
        geom = translate(pattern, xoff=dx, yoff=dy)
        
        if contains.matches(extent.relate(geom)):
            # Check that it is within the extent
            overlap = False
            for g in geoms:
                if intersects.matches(g.relate(geom)):
                    overlap = True
            if overlap == False:
                geoms.append(geom)
        
    for geom in geoms:
        outputFile.write(dumps(geom) + '\n')
    outputFile.close()

if __name__ == "__main__":
    description = "Generate a list of polygons from a pattern one by shifting it within a specified spatial extent"
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument('input', help='Input file with the pattern (as WKT)')
    parser.add_argument('extent', help='Extent file with the rectangle defining the extent (as WKT)')
    parser.add_argument('number', help='Number of generated polygons', type=int)
    parser.add_argument('output', help='Output file with the generated polygons (as WKT)')
     
    # extract user entered arguments
    opts = parser.parse_args()

    # run main
    main(opts)