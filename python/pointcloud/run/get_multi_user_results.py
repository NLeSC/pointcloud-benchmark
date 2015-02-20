#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import sys

inputFile = sys.argv[1]

lines = open(inputFile,'r').read().split('\n')

if lines[0].count('Time') == 0:
    print 'ERROR: first line must be the header and it must start with Time[s]'

ds = lines[0].split()
d = {}

for line in lines[3:]:
    if line !='':
        fields = line.split()
        for i in range(len(fields)):
            if i not in d:
                d[i] = []
            d[i].append(fields[i])

for i in range(1,len(ds)):
    for j in range(len(d[i])):
        d[i][j] = float(d[i][j])

for i in range(1,len(ds)):
    print ds[i], sum(d[i]) / len(d[i])
