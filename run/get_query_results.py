#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, sys, numpy, glob
from pointcloud.tabulate import tabulate
from pointcloud import utils

def getField(all_results, query, folder, pKey, pExpectEqual):
    if query in all_results[folder]:
        narray = numpy.array(all_results[folder][query][pKey])
        qMean = narray.mean()
        if pType == float:
            if qMean.is_integer():
                qMean = str(int(qMean))
            else:
                qMean = '%.2f' % qMean
        else:
            qMean = str(pType(qMean))
        f = qMean
        if pExpectEqual and narray.std() != 0:
            f += '*'
    else:
        f = '-'
    return f


inputFolders = sys.argv[5:]
texmode = sys.argv[1].lower() == 'tex'
orientation = sys.argv[2]
includes = sys.argv[3]
userIndexes = utils.getElements(sys.argv[4])

all_results = {}

hParams = [('n','result', 1, 'Result', float, True),('w','waste', 2, 'Waste percentage', float, True),('t','time', 3, 'Time[s]', float, False),('c','cpu', 4, 'CPU', float, False),('m','mem', 5, 'Memory' ,float, False)]

for folder in inputFolders:
    if folder.count('*'):
        subfolders = glob.glob(folder)
    else:
        subfolders = [folder,]
    
    if folder not in all_results:
        all_results[folder] = {}
    
    for subfolder in subfolders:
        f = subfolder + '/results'
        if os.path.isfile(f):
            for line in open(subfolder + '/results', 'r').read().split('\n'):
                if (not line.startswith('#')) and (not line.startswith('----')) and line != '':
                    fields = line.split()
                    query = fields[0]
                    if query not in all_results[folder]:
                        all_results[folder][query] = {}
                    for (pChar, pKey, pIndex, pName, pType, pExpectEqual) in hParams:
                        if pKey not in all_results[folder][query]:
                            all_results[folder][query][pKey] = []
                        try:
                            field = pType(fields[pIndex])
                        except:
                            field = pType(-1)
                        all_results[folder][query][pKey].append(field)

all_queries = []
for fkey in all_results:
    all_queries.extend(all_results[fkey].keys())    
all_queries = sorted(set(all_queries))

queries = []
for i in range(len(all_queries)):
    if i in userIndexes:                
        queries.append(all_queries[i])

h = []
texs = {}
rows = {}
for hIndex in range(len(hParams)):
    (pChar, pKey, pIndex, pName, pType, pExpectEqual) = hParams[hIndex]
    if includes.count(pChar):
        line = '\\textbf{' + pName + '}'
        if len(h)  == 0:
            if orientation == 'h':
                for query in queries:
                    line +=  ' & \\textbf{' + str(query) + '}'
                    h.append(str(query))
            else:
                for folder in inputFolders:
                    line +=  ' & \\textbf{' + folder + '}'
                    h.append(folder)
        else:
            if orientation == 'h':
                for query in queries:
                    line +=  ' &'
            else:
                for folder in inputFolders:
                    line +=  ' &'
        texs[pName] = ''
        texs[pName] += '\\hline' + '\n'
        texs[pName] += line + ' \\\\'+ '\n'
        texs[pName] += '\\hline'+ '\n'
        rows[pName] = []
        if orientation == 'h':
            for folder in inputFolders:
                line = folder
                r = [folder,]
                for query in queries:
                    f = getField(all_results, query, folder, pKey, pExpectEqual)
                    line += ' & ' + f
                    r.append(f)
                rows[pName].append(r)
                texs[pName] += line + ' \\\\'+ '\n'
            texs[pName] += '\\hline'+ '\n'
        else:
            for query in queries:
                line = query
                r = [query,]
                for folder in inputFolders:
                    f = getField(all_results, query, folder, pKey, pExpectEqual)
                    line += ' & ' + f
                    r.append(f)
                rows[pName].append(r)
                texs[pName] += line + ' \\\\'+ '\n'
            texs[pName] += '\\hline'+ '\n'
    
for hIndex in range(len(hParams)):
    (pChar, pKey, pIndex, pName, pType, pExpectEqual) = hParams[hIndex]
    if includes.count(pChar):
        if texmode:
            cols = []
            for i in range(len(h)):
                cols.append('r')
            tex = """\\begin{table}[!ht]
\\begin{tabular}{|l|""" + '|'.join(cols) + """|}
""" + texs[pName] + """
\\end{tabular}
\\caption{Comparison of query """ + pName + """ for the different approaches}
\\label{tab:quering""" + pName + """}
\\end{table}
"""
            print tex.replace('_','\_')
        else:
            print 
            print tabulate(rows[pName], headers=[pName,]+h)
