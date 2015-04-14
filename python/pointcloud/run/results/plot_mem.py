#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import matplotlib, sys, numpy, os
matplotlib.use('Agg')
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
from pointcloud import utils

lines = open(sys.argv[1],'r').read().split('\n')

memfrees = []
buffers = []
cacheds = []
for line in lines:
    if line != '':
        fields = line.split()
        try:
            memfree = int(fields[1])
            buffer = int(fields[2])
            cached = int(fields[3])

            memfrees.append(memfree)
            buffers.append(buffer)
            cacheds.append(cached)
        except:
            print 'skipped line!'

xs = range(len(memfrees))
memfrees = numpy.array(memfrees) / (1024. * 1024.) 
buffers = numpy.array(buffers) / (1024. * 1024.)
cacheds = numpy.array(cacheds) / (1024. * 1024.)

plots = [
('MemFree', memfrees),
('Buffers', buffers),
('Cached', cacheds),
]

fig = plt.figure(figsize = (15,7), dpi = 75)
shareX = None

for i in range(len(plots)):
    axes = fig.add_subplot(len(plots),1, i+1, sharex=shareX)
    if shareX == None:
        shareX = axes
    axes.set_ylabel(plots[i][0] + '[GB]', fontproperties=FontProperties(size="x-small"))

    ys = plots[i][1]
    axes.plot(xs, ys, alpha=0.6, linestyle = '-', color=utils.PCOLORS[i], label = plots[i][0])
    axes.autoscale(axis='x', tight=True)
    axes.autoscale(axis='y', tight=True)

axes.set_xlabel('Time[s]')

fig.savefig(sys.argv[1]+ '.png')
