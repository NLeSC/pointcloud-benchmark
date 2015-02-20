#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, time, numpy, multiprocessing, psutil, subprocess, logging, math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
#from matplotlib.font_manager import FontProperties

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
        ostr += '[' + section + ']\n'
        for option in config.options(section):
            ostr += option+":" + str(config.get(section, option)) + '\n'
    return ostr

def getFiles(inputElement, extension, recursive = False):
    """ Get the list of files with certain extension contained in the folder (and possible 
subfolders) given by inputElement. If inputElement is directly a file it 
returns a list with only one element, the given file """
    inputElementAbsPath = os.path.abspath(inputElement)
    if os.path.isdir(inputElementAbsPath):
        elements = sorted(os.listdir(inputElementAbsPath), key=str.lower)
        absPaths=[]
        for element in elements:
            elementAbsPath = os.path.join(inputElementAbsPath,element) 
            if os.path.isdir(elementAbsPath):
                if recursive:
                    absPaths.extend(getFiles(elementAbsPath, extension))
            else: #os.path.isfile(elementAbsPath)
                if elementAbsPath.endswith(extension):
                    absPaths.append(elementAbsPath)
        return absPaths
    elif os.path.isfile(inputElementAbsPath):
        if inputElementAbsPath.endswith(extension):
            return [inputElementAbsPath,]
    else:
        raise Exception("ERROR: inputElement is neither a valid folder nor file")
    return []    

def getCurrentTimeStamp(timeFormat = DEFAULT_TIMEFORMAT):
    """ Get current local time stamp """
    return str(time.strftime(timeFormat))

def getUsagePy():
    #return [time.time(), psutil.virtual_memory().percent , psutil.cpu_percent(interval=1)]
    return [time.time(), psutil.cpu_percent(), psutil.virtual_memory().percent]

def getUsageTop():
    tcpu = 0.
    tmem = 0.
    for line in os.popen("top -b -n 1 | tail -n +8 | awk '{ print $9 \" \" $10}'").read().split('\n'):
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
    return [time.time(), tcpu , tmem]

def getUsagePS():
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
    return [time.time(), tcpu , tmem]

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

def getLASParams(inputFile, tool = 'liblas'):
    if tool == 'liblas':
        outputLASInfo = subprocess.Popen('lasinfo ' + inputFile, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        for line in outputLASInfo[0].split('\n'):
            if line.count('Min X Y Z:'):
                [minX, minY, minZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('Max X Y Z:'):
                [maxX, maxY, maxZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('Actual Point Count:'):
                count = line.split(':')[-1].strip()
            elif line.count('Scale Factor X Y Z:'):
                [scaleX, scaleY, scaleZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('Offset X Y Z:'):
                [offsetX, offsetY, offsetZ] = line.split(':')[-1].strip().split(' ')
    elif tool == 'pyliblas':
        from liblas import file
        header = file.File(inputFile, mode='r').header
        [minX, minY, minZ] = header.min
        [maxX, maxY, maxZ] = header.max
        count = int(header.point_records_count)
        [scaleX, scaleY, scaleZ] = header.scale
        [offsetX, offsetY, offsetZ] = header.offset
    else:
        outputLASInfo = subprocess.Popen('lasinfo ' + inputFile + ' -nc -nv -nco', shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        for line in outputLASInfo[1].split('\n'):
            if line.count('min x y z:'):
                [minX, minY, minZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('max x y z:'):
                [maxX, maxY, maxZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('number of point records:'):
                count = line.split(':')[-1].strip()
            elif line.count('scale factor x y z:'):
                [scaleX, scaleY, scaleZ] = line.split(':')[-1].strip().split(' ')
            elif line.count('offset x y z:'):
                [offsetX, offsetY, offsetZ] = line.split(':')[-1].strip().split(' ')
    return (count, minX, minY, minZ, maxX, maxY, maxZ, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ)

def addUsage(usageMethod, output):
    if usageMethod != None:
        u = usageMethod()
        output.write('%.2f %.2f %.2f\n' % (u[0], u[1], u[2]))
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

def runMonitor(function, arguments = None, usageMethod = None, usageAbsPath = None, ioDevices = None, ioAbsPath = None):
    """ Run the function with the given arguments while checking CPU and MEM consumption"""
    usageOutputFile = open(usageAbsPath, 'w')
    if ioAbsPath != None:
        ioOutputFile = open(ioAbsPath, 'w')
        devices = initIO(ioDevices)
        ioOutputFile.write('#' + (' '.join(devices)) + '\n')
        addIO(devices, ioOutputFile)
    addUsage(usageMethod, usageOutputFile)
    if arguments != None:
        child = multiprocessing.Process(target=function, args=arguments)
    else:
        child = multiprocessing.Process(target=function)
    child.start()
    while child.is_alive():
        if ioAbsPath != None:
            addIO(devices, ioOutputFile)
        addUsage(usageMethod, usageOutputFile)
        child.join(1)  
    if ioAbsPath != None:
        addIO(devices, ioOutputFile)
        ioOutputFile.close()
    addUsage(usageMethod, usageOutputFile)
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
    
def dbExists(dbname):
    return (int(subprocess.Popen('psql -l | grep ' + dbname + ' | wc -l', shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].split('\n')[0]) > 0)

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

def oraclemogrify(cursor, query, queryArgs = None):
    query = query.upper()
    if queryArgs == None:
        return query
    else:
        cursor.prepare(query)
        bindnames = cursor.bindnames()
        if len(queryArgs) != len(bindnames):
            raise Exception('Error: len(queryArgs) != len(bindnames) \n ' + str(queryArgs) + '\n' + str(bindnames))
        if (type(queryArgs) == list) or (type(queryArgs) == tuple):
            for i in range(len(queryArgs)):
                query = query.replace(':'+bindnames[i],str(queryArgs[i]))
            return query
        elif type(queryArgs) == dict:
            upQA = {}
            for k in queryArgs:
                upQA[k.upper()] = queryArgs[k]
            for bindname in bindnames:
                query = query.replace(':'+bindname, str(upQA[bindname]))
            return query
        else:
            raise Exception('Error: queryArgs must be dict, list or tuple')

def monetdbmogrify(cursor, query, queryArgs = None):
    if queryArgs == None:
        return query
    else:
        pquery = query
        for qa in queryArgs:
            qindex = pquery.index('%s')
            pquery = pquery[:qindex] + str(qa) + pquery[qindex+2:]
        return pquery
             
def las2txtCommand(inputFile, outputFile = 'stdout', columns = 'xyz', separation = None, tool = 'liblas'):
    separatorArgument = ''
    if separation != None:
        if tool in ('lastools','lasnlesc'):
            cd = {' ':'space', '\t':'tab', ',':'comma', ':':'colon', '-':'hyphen', '.':'dot', ';':'semicolon'}
            if separation in cd:
                separatorArgument = ' -sep ' + cd[separation] + ' '
            else:
                raise Exception('ERROR: separation ' + separation + ' not supported in ' + tool + '!')
        else:
            separatorArgument = ' --delimiter "' + separation + '" '

    if tool == 'liblas':
        c = 'las2txt --input ' + inputFile + ' --output ' + outputFile + ' --parse ' + columns + separatorArgument
    elif tool == 'lastools':
        c = 'las2txt -i ' + inputFile + ' -o ' + outputFile + ' -parse ' + columns + separatorArgument
    elif tool == 'lasnlesc':
        c = 'las2txt -i ' + inputFile + ' --stdout --parse ' + columns + separatorArgument
        if outputFile != 'stdout':
            c += ' > ' + outputFile
    else:
        raise Exception('ERROR: unknown las2txt tool (' + tool + ')')
        
    return c       

def postgresConnectString(dbName = None, userName= None, password = None, dbHost = None, dbPort = None, cline = False):
    connString=''
    if cline:    
        if dbName != None and dbName != '':
            connString += " " + dbName
        if userName != None and userName != '':
            connString += " -U " + userName
        if password != None and password != '':
            os.environ['PGPASSWORD'] = password
        if dbHost != None and dbHost != '':
            connString += " -h " + dbHost
        if dbPort != None and dbPort != '':
            connString += " -p " + dbPort
    else:
        if dbName != None and dbName != '':
            connString += " dbname=" + dbName
        if userName != None and userName != '':
            connString += " user=" + userName
        if password != None and password != '':
            connString += " password=" + password
        if dbHost != None and dbHost != '':
            connString += " host=" + dbHost
        if dbPort != None and dbPort != '':
            connString += " port=" + dbPort
    
    return connString

def getPostgresSizes(cursor):
    """ Return tuple with size of indexes, size excluding indexes, and total size (all in MB)"""
    cursor.execute("""SELECT sum(pg_indexes_size(tablename::text)) / (1024*1024) size_indexes,  sum(pg_table_size(tablename::text)) / (1024*1024) size_ex_indexes, sum(pg_total_relation_size(tablename::text)) / (1024*1024) size_total FROM pg_tables where schemaname='public'""")
    return list(cursor.fetchone())

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
