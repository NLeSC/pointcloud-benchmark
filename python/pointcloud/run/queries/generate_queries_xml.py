#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import argparse

def main(opts):
    wkts = open(opts.input, 'r').read().split('\n')
    outputFile = open(opts.output, 'w')
    outputFile.write('<?xml version="1.0" encoding="UTF-8"?>' + '\n')
    outputFile.write('<queries>' + '\n')
    
    numWKTs = len(wkts)
    
    counter = 0
    for i in range(numWKTs):
        wkt = wkts[i]
        if wkt != '' and (opts.number < 0 or (counter < opts.number)):
            counter+=1
            qid = ('%0' + str(len(str(numWKTs))) + 'd') % counter
            outputFile.write('    <query id="' + qid + '" key="' + opts.name + '_' + qid + '">' + '\n')
            outputFile.write('        <type>' + opts.type + '</type>' + '\n')
            outputFile.write('        <wkt>' + '\n')
            outputFile.write('            ' + wkt + '\n')
            outputFile.write('        </wkt>' + '\n')
            outputFile.write('        <columns>' + opts.columns + '</columns>' + '\n')
            outputFile.write('    </query>' + '\n')
        
    outputFile.write('</queries>' + '\n')

if __name__ == "__main__":
    description = "Generate a XML with the queries from a file generated with generate_queries"
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument('input', help='Input file with the query geometries (in WKT)')
    parser.add_argument('type', help='Type of queries to be specified in the XML')
    parser.add_argument('name', help='Common name to be used when generating the keys')
    parser.add_argument('output', help='Output XML file')
    parser.add_argument('-c', '--columns', help='Columns', default='xyz')
    parser.add_argument('-n', '--number', help='Only output the first N geometries', default='-1', type=int)
     
    # extract user entered arguments
    opts = parser.parse_args()

    # run main
    main(opts)