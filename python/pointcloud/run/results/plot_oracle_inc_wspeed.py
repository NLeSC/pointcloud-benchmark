#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import matplotlib, sys, numpy, os
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pointcloud import utils

inputFile = os.path.abspath(sys.argv[1])
command = 'cat ' + inputFile + ' | grep "Read "'
lines = utils.shellExecute(command).split('\n')

nums = []
reads = []
sorts = []
writes = []
for line in lines:
    if line != '':
        fields = line.split()
        try:
            num = float(fields[1].replace(',','')) / 1000000.
            read = float(fields[3].replace(',',''))
            sort = float(fields[6].replace(',',''))
            write = float(fields[9].replace(',',''))
            nums.append(num)
            reads.append(read)
            sorts.append(sort)
            writes.append(write)
        except:
            print 'skipped line!'

xs = range(1,len(nums)+1)
nums = numpy.array(nums)
reads = numpy.array(reads) 
sorts = numpy.array(sorts) 
writes = numpy.array(writes) 
totals = reads+sorts+writes

print
print '#Files = ' + str(len(nums)) 
print '#Points = ' + str(int(sum(nums))) + ' Mpts'

plots = [
('Reads', reads),
('Sorts', sorts),
('Writes', writes),
('Totals', totals),
]

fig = plt.figure(figsize = (15,7), dpi = 75)
ax1 = fig.add_subplot(111)
ax2 = ax1.twiny()

labels = []
rects = []
for i in range(len(plots)):
    ys = nums / plots[i][1]
    avg = ys[ys != numpy.inf].mean()
    print 'Avg. ' + plots[i][0] + ' = ' + ('%.2f' % avg)
#    if len(ys) > 10:
#        print 'Last ' + plots[i][0] + ' = ' , ys[-10:-1]
#    else:
#        print 'Last ' + plots[i][0] + ' = ' + ('%.2f' % ys[-1])
    print 'Last ' + plots[i][0] + ' = ' + ('%.2f' % ys[-1])
    ax1.plot(xs, ys, alpha=0.6, linestyle = '-', color=utils.PCOLORS[i], label = plots[i][0])
    rects.append(plt.Rectangle((0, 0), 1, 1, fc=utils.PCOLORS[i]))    
    labels.append(plots[i][0])

ax1.set_xlabel('File counter')
ax1.set_ylabel('Mpts/s')

if len(sys.argv) > 2:
    (miny,maxy) = sys.argv[2].split(',')
    ax1.set_ylim([float(miny), float(maxy)])
ax1.autoscale(axis='x', tight=True)

def cumulative_sum(values, start=0): 
    for v in values: 
        start += v 
        yield start 
        
anums =  list(cumulative_sum(nums)) 
ax2.plot(anums, anums) # Create a dummy plot
ax2.cla()
ax2.set_xlabel("#Mpts")
ax2.autoscale(axis='x', tight=True)

title = ax1.set_title("Oracle blocks loading")
title.set_y(1.1)
fig.subplots_adjust(top=0.85)

plt.legend(rects, labels)

#fig.gca().legend()
fig.savefig(inputFile+ '.png')
