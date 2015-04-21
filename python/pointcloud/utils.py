#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, time, numpy, multiprocessing, psutil, subprocess, logging, math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


RESULTS_FILE_NAME = 'results'
QUERY_TABLE = 'query_polygons'

DEFAULT_TIMEFORMAT = "%Y/%m/%d/%H:%M:%S"

PCOLORS = [(0.0, 0.0, 1.0), (0.0, 1.0, 0.0), (1.0, 0.0, 0.0), (0.0, 0.75, 0.75), 
           (0.75, 0.0, 0.75), (0.75, 0.75, 0.0), (0.0, 1.0, 1.0),
           (1.0, 1.0, 0.0), (1.0, 1.0, 1.0), (0.0, 0.0, 0.25), (1.0, 0.0, 1.0), (0.0, 0.0, 0.5), (0.0, 0.0, 0.75), 
           (0.0, 0.25, 0.0), (0.0, 0.25, 0.25), (0.0, 0.25, 0.5), (0.0, 0.25, 0.75), (0.0, 0.25, 1.0), 
           (0.0, 0.5, 0.0), (0.0, 0.5, 0.25), (0.0, 0.5, 0.5), (0.0, 0.5, 0.75), (0.0, 0.5, 1.0), 
           (0.0, 0.75, 0.0), (0.0, 0.75, 0.25), (0.0, 0.75, 0.5), (0.0, 0.75, 1.0), (0.0, 1.0, 0.25), 
           (0.0, 1.0, 0.5), (0.0, 1.0, 0.75), (0.25, 0.0, 0.0), (0.25, 0.0, 0.25), (0.25, 0.0, 0.5), 
           (0.25, 0.0, 0.75), (0.25, 0.0, 1.0), (0.25, 0.25, 0.0), (0.25, 0.25, 0.25), (0.25, 0.25, 0.5), 
           (0.25, 0.25, 0.75), (0.25, 0.25, 1.0), (0.25, 0.5, 0.0), (0.25, 0.5, 0.25), (0.25, 0.5, 0.5), 
           (0.25, 0.5, 0.75), (0.25, 0.5, 1.0), (0.25, 0.75, 0.0), (0.25, 0.75, 0.25), (0.25, 0.75, 0.5), 
           (0.25, 0.75, 0.75), (0.25, 0.75, 1.0), (0.25, 1.0, 0.0), (0.25, 1.0, 0.25), (0.25, 1.0, 0.5),
            (0.25, 1.0, 0.75), (0.25, 1.0, 1.0), (0.5, 0.0, 0.0), (0.5, 0.0, 0.25), (0.5, 0.0, 0.5),
             (0.5, 0.0, 0.75), (0.5, 0.0, 1.0), (0.5, 0.25, 0.0), (0.5, 0.25, 0.25), (0.5, 0.25, 0.5), 
             (0.5, 0.25, 0.75), (0.5, 0.25, 1.0), (0.5, 0.5, 0.0), (0.5, 0.5, 0.25), (0.5, 0.5, 0.5), 
             (0.5, 0.5, 0.75), (0.5, 0.5, 1.0), (0.5, 0.75, 0.0), (0.5, 0.75, 0.25), (0.5, 0.75, 0.5), 
             (0.5, 0.75, 0.75), (0.5, 0.75, 1.0), (0.5, 1.0, 0.0), (0.5, 1.0, 0.25), (0.5, 1.0, 0.5), 
             (0.5, 1.0, 0.75), (0.5, 1.0, 1.0), (0.75, 0.0, 0.0), (0.75, 0.0, 0.25), (0.75, 0.0, 0.5), 
             (0.75, 0.0, 1.0), (0.75, 0.25, 0.0), (0.75, 0.25, 0.25), (0.75, 0.25, 0.5), (0.75, 0.25, 0.75),
              (0.75, 0.25, 1.0), (0.75, 0.5, 0.0), (0.75, 0.5, 0.25), (0.75, 0.5, 0.5), (0.75, 0.5, 0.75), 
              (0.75, 0.5, 1.0), (0.75, 0.75, 0.25), (0.75, 0.75, 0.5), (0.75, 0.75, 0.75), (0.75, 0.75, 1.0), 
              (0.75, 1.0, 0.0), (0.75, 1.0, 0.25), (0.75, 1.0, 0.5), (0.75, 1.0, 0.75), (0.75, 1.0, 1.0), (1.0, 0.0, 0.25), 
              (1.0, 0.0, 0.5), (1.0, 0.0, 0.75), (1.0, 0.25, 0.0), (1.0, 0.25, 0.25), (1.0, 0.25, 0.5), (1.0, 0.25, 0.75), 
              (1.0, 0.25, 1.0), (1.0, 0.5, 0.0), (1.0, 0.5, 0.25), (1.0, 0.5, 0.5), (1.0, 0.5, 0.75), (1.0, 0.5, 1.0), 
              (1.0, 0.75, 0.0), (1.0, 0.75, 0.25), (1.0, 0.75, 0.5), (1.0, 0.75, 0.75), (1.0, 0.75, 1.0), (1.0, 1.0, 0.25), 
              (1.0, 1.0, 0.5), (1.0, 1.0, 0.75),]

PC_FILE_FORMATS = ['las','laz','LAS', 'LAZ']

# This module define some useful functions

def chunkedMean(values, resolution):
    s = int(math.ceil(len(values) / float(resolution)))
    return numpy.array([sum(group)/float(len(group)) for group in numpy.array_split(values,s)])

def getUserName():
    return os.popen('whoami').read().replace('\n','')

def setOptions(optionsString, config):
    if optionsString != '':
        for option in optionsString.strip().split(' '):
            if option != '':
                (section,option,value) = option.split(':')
                if option in config.options(section):
                    config.set(section,option,value)
                else:
                    raise Exception(option + ' is not a valid option')
                
def showOptions(config, sections = None):
    ostr = ''
    if sections == None:
        sections = config.sections()
    for section in sections:
        if section in config.sections():
            ostr += '[' + section + ']\n'
            for option in config.options(section):
                ostr += option+":" + str(config.get(section, option)) + '\n'
    return ostr

def getFiles(inputElement, extensions = PC_FILE_FORMATS, recursive = False):
    """ Get the list of files with certain extensions contained in the folder (and possible 
subfolders) given by inputElement. If inputElement is directly a file it 
returns a list with only one element, the given file """

    # Is extensions is not a list but a string we converted to a list
    if type(extensions) == str:
        extensions = [extensions,]

    inputElementAbsPath = os.path.abspath(inputElement)
    if os.path.isdir(inputElementAbsPath):
        elements = sorted(os.listdir(inputElementAbsPath), key=str.lower)
        absPaths=[]
        for element in elements:
            elementAbsPath = os.path.join(inputElementAbsPath,element) 
            if os.path.isdir(elementAbsPath):
                if recursive:
                    absPaths.extend(getFiles(elementAbsPath, extensions))
            else: #os.path.isfile(elementAbsPath)
                isValid = False
                for extension in extensions:
                    if elementAbsPath.endswith(extension):
                        isValid = True
                if isValid:
                    absPaths.append(elementAbsPath)
        return absPaths
    elif os.path.isfile(inputElementAbsPath):
        isValid = False
        for extension in extensions:
            if inputElementAbsPath.endswith(extension):
                isValid = True
        if isValid:
            return [inputElementAbsPath,]
    else:
        raise Exception("ERROR: inputElement is neither a valid folder nor file")
    return []    

def initIO(devices = None):
    d = sorted(psutil.disk_io_counters(True).keys())
    if devices == None:
        return d
    else:
        for device in devices:
            if device not in d:
                raise Exception('ERROR: Device ' + device + ' not found')
        return devices

def getIO(devices):
    iodata = psutil.disk_io_counters(True)
    r = [time.time(), ]
    for device in devices:
       r.append(iodata[device].read_bytes)
       r.append(iodata[device].write_bytes)
    return r

def addUsage(usageMonitor, output):
    if usageMonitor:
        tcpu = 0.
        tmem = 0.
        for line in os.popen("ps aux | tail -n +2 | awk '{ print $3 \" \" $4}'").read().split('\n'):
            if line != '':
                fields = line.strip().split(' ')
                if len(fields) == 2:
                    try:
                        cpu = float(fields[0])
                        mem = float(fields[1])
                    except:
                        cpu = 0.
                        mem = 0.
                    tcpu += cpu
                    tmem += mem
        output.write('%.2f %.2f %.2f\n' % (time.time(), tcpu , tmem))
        output.flush()
    
def addIO(devices, output):
    u = getIO(devices)
    t = [('%.2f' % u[0]), ]
    for i in range(1, len(u)):
        t.append('%d' % u[i])
    output.write(' '.join(t) + '\n')
    output.flush()

def parseUsage(outputAbsPath):
    usage = numpy.loadtxt(outputAbsPath)
    return (usage[:,0], usage[:,1], usage[:,2])

def parseIO(outputAbsPath):
    devices = open(outputAbsPath, 'r').readline().replace('#','').replace('\#','').split()
    usage = numpy.loadtxt(outputAbsPath)
    times = usage[:,0]
    rdata = {}
    wdata = {}
    for i in range(len(devices)):
        rdata[devices[i]] = usage[:,(1 + (2*i))]
        wdata[devices[i]] = usage[:,(2 + (2*i))]
    return (times, rdata, wdata)

def runMonitor(function, arguments = None, usageMonitor = False, usageAbsPath = None, ioDevices = None, ioAbsPath = None):
    """ Run the function with the given arguments while checking CPU and MEM consumption"""
    usageOutputFile = open(usageAbsPath, 'w')
    if ioAbsPath != None:
        ioOutputFile = open(ioAbsPath, 'w')
        devices = initIO(ioDevices)
        ioOutputFile.write('#' + (' '.join(devices)) + '\n')
        addIO(devices, ioOutputFile)
    addUsage(usageMonitor, usageOutputFile)
    if arguments != None:
        child = multiprocessing.Process(target=function, args=arguments)
    else:
        child = multiprocessing.Process(target=function)
    child.start()
    while child.is_alive():
        if ioAbsPath != None:
            addIO(devices, ioOutputFile)
        addUsage(usageMonitor, usageOutputFile)
        child.join(1)  
    if ioAbsPath != None:
        addIO(devices, ioOutputFile)
        ioOutputFile.close()
    addUsage(usageMonitor, usageOutputFile)
    usageOutputFile.close()

def saveUsage(times, cpus, mems, title, outputFileName):
    try:
        fig, ax1 = plt.subplots(figsize = (15,7), dpi = 75)
        t = times - times[0]
    
        ax1.plot(t, cpus, 'b')
        ax1.set_xlabel('Time[s]')
        # Make the y-axis label and tick labels match the line color.
        ax1.set_ylabel('%CPUs', color='b')
        for tl in ax1.get_yticklabels():
            tl.set_color('b')
    
        ax2 = ax1.twinx()
        ax2.plot(t, mems, 'r')
        ax2.set_ylabel('%MEM', color='r')
        for tl in ax2.get_yticklabels():
            tl.set_color('r')

        try:
            ax1.set_title(title)
            ax1.autoscale(axis='x', tight=True)
        except:
            logging.warning('Matplotlib too old...')

        fig.savefig(outputFileName)
        plt.close()
    except:
        logging.error('ERROR: Could not save usage image!')
    
def saveIO(times, rdata, wdata, title, outputFileName):
    try:
        fig, ax1 = plt.subplots(figsize = (15,7), dpi = 75)
        t = times - times[0]
    
        devices = sorted(rdata.keys())
        for i in range(len(devices)):
            device = devices[i]
            r = ((rdata[device][1:] - rdata[device][:-1]) / (t[1:] - t[:-1])) / 1048576.
            w = ((wdata[device][1:] - wdata[device][:-1]) / (t[1:] - t[:-1])) / 1048576.
            ax1.plot(t[1:], r, alpha=0.6, linestyle = '-', color=PCOLORS[i], label = device + ' read')
            ax1.plot(t[1:], w, alpha=0.6, linestyle = '--', color=PCOLORS[i], label = device + ' write')

        ax1.set_xlabel('Time[s]')
        ax1.set_ylabel('IO[MBps]')
    
        try:
            ax1.set_title(title)
            ax1.autoscale(axis='x', tight=True)
        except:
            logging.warning('Matplotlib too old...')
        fig.gca().legend()
        fig.savefig(outputFileName)
        plt.close()
    except:
        logging.error('ERROR: Could not save IO image!')
    
def sizeof_fmt(num):
    try:
        for x in ['bytes','KB','MB','GB']:
            if num < 1024.0 and num > -1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0
        return "%3.1f %s" % (num, 'TB')
    except:
        return ''
    
def getNRowNCol(v, nummax = 10):
    d = []
    for i in range(nummax):
        n = nummax - i
        if (v % n) == 0:
            a = v / n
            b = v / a
            if a > b:
                d.append((b,a))
            else:
                d.append((a,b))
    if len(d) == 0:
        raise Exception('Error getting row col')
    return max(d)
             
def getElements(rangeString):
    elements = []
    for e in rangeString.split(','):
        isRange = False
        for rangeSeparator in ('-', '..', ':'):
            if e.count(rangeSeparator):
                isRange = True
                erange = e.split(rangeSeparator)
                if(len(erange) not in (2,3)) or (int(erange[0]) > int(erange[1])):
                    raise Exception('Invalid Range: ' + e) 
                step = 1
                if len(erange) == 3:
                    step = int(erange[2])
                for i in range(int(erange[0]),(1 + int(erange[1])), step):
                    elements.append(i)
        if not isRange:
            elements.append(int(e))
    return elements

def writeToFile(outputFileName, content):
    outputFile = open(outputFileName, 'w')
    outputFile.write(content)
    outputFile.close()
    return outputFileName

def shellExecute(command):
    logging.debug(command)
    (out,err) = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return '\n'.join((out,err))    
