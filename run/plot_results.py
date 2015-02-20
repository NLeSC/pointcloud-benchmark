#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
from matplotlib.font_manager import FontProperties

inputFile = sys.argv[1]
outputFile = sys.argv[2]

# first line is header, second is not used
lines = open(inputFile,'r').read().split('\n')

header = lines[1]
hfields = header.split()

if len(hfields) < 2:
    print 'Wrong number of columns!'
    sys.exit(1)
    
param = hfields[0]

columns = []
for i in range(1,len(hfields)):
    columns.append(hfields[i])
    

xsnames = []
xs = []

qss = {}
for i in range(len(columns)):
    qss[columns[i]] = []

c = 0
for line in lines[3:]:
    if line != '':
        fields = line.split()
        xsnames.append(fields[0])
        xs.append(c)
        c += 1
        for i in range(len(columns)):
            qss[columns[i]].append(float(fields[i+1]))
            

fig = plt.figure(figsize = (15,7), dpi = 75)

shareX = None

for i in range(len(columns)):
    axes = fig.add_subplot(len(columns),1, i+1, sharex=shareX)
    if shareX == None:
        shareX = axes
    if i == 0:
        axes.set_ylabel(param, fontproperties=FontProperties(size="x-small"))
    axes.set_title(columns[i], fontproperties=FontProperties(size="x-small"))
     
    axes.plot(xs, qss[columns[i]], alpha=0.6, marker='.' ,linestyle = '-', color='r', markeredgecolor = 'r', markersize = 2.)

    axes.autoscale(axis='x', tight=True)
    axes.autoscale(axis='y', tight=True)
    
    for label in axes.get_xticklabels() + axes.get_yticklabels():
        label.set_fontsize(8)
    
    def formatStamp(x, pos=None):
        return xsnames[int(x)]
    
    axes.xaxis.set_major_formatter(ticker.FuncFormatter(formatStamp))

#fig.gca().legend(prop=FontProperties(size="x-small"), markerscale=1.)
fig.autofmt_xdate()  
try:
    plt.tight_layout(pad=0.2, h_pad=0.12, w_pad=0.12)
except:
    fig.subplots_adjust(top=0.97)
    fig.subplots_adjust(right=0.98)
    fig.subplots_adjust(left=0.03)
    fig.subplots_adjust(bottom=0.09)
    fig.subplots_adjust(hspace=0.06)
    fig.subplots_adjust(wspace=0.07)
fig.savefig(outputFile)
