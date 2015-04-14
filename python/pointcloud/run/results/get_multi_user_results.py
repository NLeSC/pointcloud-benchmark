#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import sys
from pointcloud.tabulate import tabulate

resultsName = sys.argv[1]
inputFiles = sys.argv[2:]

d = {}
ds = None

for inputFile in inputFiles:

    lines = open(inputFile + '/' + resultsName,'r').read().split('\n')

    if lines[0].count('Time') == 0:
        print 'ERROR: first line must be the header and it must start with Time[s]'
    if ds == None:
        ds = lines[0].split()

    for line in lines[3:]:
        if line !='':
            fields = line.split()
            for i in range(len(fields)):
                if (i,inputFile) not in d:
                    d[(i,inputFile)] = []
                d[(i,inputFile)].append(fields[i])

    for i in range(1,len(ds)):
        for j in range(len(d[(i,inputFile)])):
            d[(i,inputFile)][j] = float(d[(i,inputFile)][j])

rows = []
h = ['QuUs',] + inputFiles
print '\t'.join(h)
for i in range(1,len(ds)):
    es = [str(ds[i]),]
    for inputFile in inputFiles:
        es.append('%.03f' % (sum(d[(i,inputFile)]) / len(d[(i,inputFile)])))
    rows.append(es)
    print '\t'.join(es)


h = ['QuUs',] + inputFiles
print tabulate(rows, headers=h)
