#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, sys, numpy
from pointcloud.tabulate import tabulate
from pointcloud import utils

inputFolders = sys.argv[2:]
mode = sys.argv[1].lower()

all_results = {}

someUsage = False
for folder in inputFolders:
    if folder not in all_results:
        all_results[folder] = {}
    if os.path.isfile(folder + '/loading_results'):
        for line in open(folder + '/loading_results', 'r').read().split('\n'):
            fields = line.split(' ')
            if line.count('initialize:'):
                all_results[folder]['initialize'] = fields[-2]
            if line.count('close:'):
                all_results[folder]['close'] = fields[-2]
            if line.count('total:'):
                all_results[folder]['time'] = fields[1]
                total = float(all_results[folder]['time'])
                init = float(all_results[folder]['initialize'])
                close = float(all_results[folder]['close'])
                all_results[folder]['load'] = '%.2f' % (total - (init+close))
            if line.count('Size'):
                all_results[folder]['totalsize'] = fields[-2]
                all_results[folder]['indexsize'] = fields[3]
    #        if line.count('Avg. CPU:'):
    #            all_results[folder]['cpu'] = '.'.join(fields[2].split('.')[:-1])
    #            all_results[folder]['mem'] = fields[-2]
            if line.count('Total Num. points:'):
                all_results[folder]['npoints'] = fields[-1]
        if os.path.isfile(folder + '/loading.usage'):
            someUsage = True
            all_results[folder]['usage'] = utils.parseUsage(folder + '/loading.usage')

h = ('Approach', 'Total[s]', 'Init.[s]' , 'Load[s]', 'Close[s]', 'Total[MB]', 'Index[MB]', 'Points', 'Points/s', 'Points/MB')

tex = """
\\begin{table}[!ht]
\\centering
\\begin{tabular}{|lrrrrrrrrr|}
\\hline
\multirow{2}{*}{Approach} & \multicolumn{4}{c}{Time[s]} & \multicolumn{2}{c}{Size[MB]} & \multirow{2}{*}{Points}  & \multirow{2}{*}{Points/s} & \multirow{2}{*}{Points/MB}\\\\  
& Total & Init. & Load & Close & Total & Index & & & \\\\ 
\\hline
"""
rows = []
for folder in inputFolders:
    aux = []
    for k in ('time', 'initialize','load', 'close','totalsize','indexsize', 'npoints', 'npointspers', 'npointpermb'):
        f = None
        try:
            if k == 'npoints':
                np = 0
                if 'npoints' in all_results[folder]:
                    np = int(all_results[folder]['npoints'])
                f = '%d' % np
            elif k == 'npointspers':
                np = 0.
                if 'npoints' in all_results[folder]:
                    np = float(all_results[folder]['npoints'])
                f = '%d' % int(round((np / float(all_results[folder]['time']))))
            elif k == 'npointpermb':
                np = 0.
                if 'npoints' in all_results[folder]:
                    np = float(all_results[folder]['npoints'])
                f = '%d' % int(round((np / float(all_results[folder]['totalsize']))))
            else:
                f = '%.2f' % float(all_results[folder][k])
        except:
            f = '-'
        aux.append(f)
    rows.append(([folder,] + aux))
    tex += '\t & \t'.join(([folder,] + aux)) + ' \\\\ \n'
tex += """
\\hline
\\end{tabular}
\\caption{Times and sizes of the data loading procedure for the different approaches}
\\label{tab:loading}
\\end{table}
"""
if mode == 'tex':
    print tex.replace('_','\_')
elif mode == 'csv':
    print ','.join(h)
    for row in rows:
        print ','.join(row)
else:
    print tabulate(rows, headers=h)


def getStatValue(a, op, s = None):
    try:
        if s is None:
            na = a
        else:
            na = a[s]
        if na.size != 0:
            if op == 'mean':
                return '%.2f' % na.mean()
            elif op == 'min':
                return '%.2f' % na.min()
            elif op == 'max':
                return '%.2f' % na.max()
    except:
        pass
    return '-'

usages = []
if someUsage:
    usages = ('CPU', 'MEM')

for usage in usages:
    
    h = ('Approach','Total', 'Total-' , 'Total+', 'Init.', 'Init.-', 'Init.+', 'Load','Load-','Load+','Close','Close-','Close+')
    
    tex = """
    \\begin{table}[!ht]
    \\centering
    \\begin{tabular}{|lrrrrrrrrrrrr|}
    \\hline
    \multirow{2}{*}{Approach} & \multicolumn{3}{c}{""" + usage + """ Total} &  \multicolumn{3}{c}{""" + usage + """ Init.} &  \multicolumn{3}{c}{""" + usage + """ Load} &  \multicolumn{3}{c}{""" + usage + """ Close}\\\\  
    & Avg. & Min. & Max. & Avg. & Min. & Max. & Avg. & Min. & Max. & Avg. & Min. & Max. \\\\ 
    \\hline
    """
    rows = []
    for folder in inputFolders:
        try:
            (t,c,m) = all_results[folder]['usage']
            tr = t - t[0]
            
            init = float(all_results[folder]['initialize'])
            load = float(all_results[folder]['load'])
            
            if usage == 'CPU':
                values = c
            else:
                values = m
                
            totavg = getStatValue(values, 'mean')
            totmin = getStatValue(values, 'min')
            totmax = getStatValue(values, 'max')
            
            btini = (tr <= init)
            iniavg = getStatValue(values, 'mean',btini)
            inimin = getStatValue(values, 'min',btini)
            inimax = getStatValue(values, 'max',btini)

            btload =((init+load) >= tr) & (tr > init)
            loadavg = getStatValue(values, 'mean',btload)
            loadmin = getStatValue(values, 'min',btload)
            loadmax = getStatValue(values, 'max',btload)
            
            btclose = ((init+load) < tr)
            closeavg = getStatValue(values, 'mean',btclose)
            closemin = getStatValue(values, 'min',btclose)
            closemax = getStatValue(values, 'max',btclose)
            
            aux = [totavg, totmin, totmax, iniavg, inimin, inimax, loadavg, loadmin, loadmax, closeavg, closemin, closemax]
        except:
            aux = ['-','-','-','-','-','-','-','-','-','-','-','-']
        
        rows.append(([folder,] + aux))
        tex += '\t & \t'.join(([folder,] + aux)) + ' \\\\ \n'
    
    tex += """
    \\hline
    \\end{tabular}
    \\caption{Used """ + usage + """ of the data loading procedure for the different approaches}
    \\label{tab:loading""" + usage + """}
    \\end{table}
    """
    if mode == 'tex':
        print tex.replace('_','\_')
    elif mode == 'csv':
        print ','.join(h)
        for row in rows:
            print ','.join(row)
    else:
        print
        print usage, 'Avg.', 'Min.', 'Max.'
        print tabulate(rows, headers=h)





