#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, time, multiprocessing, logging, sys
from pointcloud import utils

class AbstractLoader:
    """Abstract class for the loaders to be implemented for each different 
    solution for the benchmark"""
    def __init__(self, configuration):
        """ Create the loader, a ConfigParser is required (loaded from a .ini file)"""
        self.conf = configuration
    
    def getConfiguration(self):
        """ Gets the configuration (ConfigParser) instance """
        return self.conf
    
    def run(self):
        """ Executes the load method while monitoring performance """
        config = self.getConfiguration()
        # Create and move to the execution path
        executionPath = config.get('General','ExecutionPath')
        if not os.path.isdir(executionPath):
            os.makedirs(executionPath)
        os.chdir(executionPath)
        usageMonitor = config.getboolean('General','UsageMonitor')
        ioMonitorParam = config.get('General','IOMonitor').strip()
        ioMonitor = False
        ioDevices = None
        if ioMonitorParam != '':
            ioMonitor = True
            ioDevices = ioMonitorParam.split(',')
        
        inputFolder = config.get('Load','Folder')
        
        resultsFileAbsPath = os.path.abspath('loading_results')
        usageAbsPath = os.path.abspath('loading.usage')
        usageImageAbsPath = os.path.abspath('loading_usage.png')
        ioAbsPath = None
        if ioMonitor:
            ioAbsPath = os.path.abspath('loading.io')
            ioImageAbsPath = os.path.abspath('loading_io.png')
        
        t0 = time.time()
        utils.runMonitor(self.load, (resultsFileAbsPath,), usageMonitor, usageAbsPath, ioDevices, ioAbsPath)
        totalTime = time.time()-t0
        cpu = '-'
        mem = '-'
        if usageMonitor:
            (times, cpus, mems) = utils.parseUsage(usageAbsPath)
            utils.saveUsage(times, cpus, mems, 'Loading CPU/MEM', usageImageAbsPath)
            cpu = '%.2f' % cpus.mean()
            mem = '%.2f' % mems.mean()
        if ioMonitor:
            (times, rdata, wdata) = utils.parseIO(ioAbsPath)
            utils.saveIO(times, rdata, wdata, 'Loading IO', ioImageAbsPath)
        resultsFile = open(resultsFileAbsPath,'a')
        logging.info('Finished loading from' + inputFolder)
        size = self.size()
        numpoints = 'Total Num. points: ' + str(self.getNumPoints())
        logging.info('Total time is: %.2f seconds' % totalTime)
        resultsFile.write('total: %.2f \n'%totalTime)
        logging.info(size)
        resultsFile.write(str(size) + '\n')
        logging.info(numpoints)
        resultsFile.write(str(numpoints) + '\n')
        logging.info('Avg. CPU: ' + cpu + '. Avg. Memory: ' + mem)
        resultsFile.write('Avg. CPU: '+ cpu + '. Avg. Memory: \n' + mem)
        resultsFile.close()
        logging.info('See ' + resultsFileAbsPath)
        logging.info('See ' + usageAbsPath)
        logging.info('See ' + usageImageAbsPath)
        if ioMonitor:
            logging.info('See ' + ioAbsPath)
            logging.info('See ' + ioImageAbsPath)

        
    def load(self, resultsFileName):
        """ Loads the data """
        resultsFile = open(resultsFileName,'w')
        # Run the initialization
        ti = time.time()
        self.initialize()
        t = time.time() - ti
        logging.info('initialize finished in ' + ('%.2f' % t) + ' seconds')
        resultsFile.write('  initialize: %.2f \n'%t)
        
        # Runs process which will process al input data
        results = self.process()
        for (inputItem, t) in results:
            resultsFile.write('    ' + inputItem + (': %.2f \n'%t))
        
        # Run close
        ti = time.time()
        self.close()
        t = time.time() - ti
        logging.info('close finished in ' + ('%.2f' % t) + ' seconds')
        resultsFile.write('  close: %.2f \n'%t)
        resultsFile.close()
        
    def logProcessItemInfo(self, inputItem, elapsedTime, itemCounter, numItems):
        logging.info('load from ' + inputItem + ' finished in ' + ('%.2f' % elapsedTime) + ' seconds. ' + str(itemCounter+1) + '/' + str(numItems) + ' (' + ('%.2f' % (float(itemCounter+1) * 100. / float(numItems))) + '%)')
    
    def processSingle(self, inputItems, processItem):
        """ Process the input items with a single core/process"""
        numItems = len(inputItems)
        if numItems == 0:
            raise Exception('No files/folders found with inputFolder/Extension')

        results = []
        for i in range(numItems):
            inputItem = inputItems[i]
            ti = time.time()
            processItem(i, inputItem)
            t = time.time() - ti
            self.logProcessItemInfo(inputItem, t, i, numItems)
            results.append([inputItem, t])
        return results
    
    def processMulti(self, inputItems, numProcessesLoad, processItemParallelMethod, processItemSequentialMethod = None, assertOrderSequentialMethod = False):
        """ Process the input items with multiple cores/processes. 
The method that is parallelized is given by processItemParallelMethod.
If provided processItemSequentialMethod, this method is executed not-parallely after each parallel process is finished (while parent process is waiting for the children processes)
If assertOrderSequentialMethod = True we assert that the processItemSequentialMethod is executed in same order as input items
"""
        numItems = len(inputItems)
        if numItems == 0:
            raise Exception('No files/folders found with inputFolder/Extension') 
        # We create a tasts queue and fill it with as many tasks as input files
        childrenQueue = multiprocessing.Queue()
        for i in range(numItems):
            inputItem = inputItems[i]
            childrenQueue.put([i, inputItem])
        for i in range(numProcessesLoad): #we add as many None jobs as self.numProcessesLoad to tell them to terminate (queue is FIFO)
            childrenQueue.put(None)
            
        resultQueue = multiprocessing.Queue() # The queue where to put the results
        children = []
        # We start numProcessesLoad children processes
        for i in range(numProcessesLoad):
            children.append(multiprocessing.Process(target=self.runChildLoad,
                args=(i, childrenQueue, resultQueue, processItemParallelMethod)))
            children[-1].start()
        
        # Collect the results
        results = []
        if assertOrderSequentialMethod:
            nextFile = 0
            finishedItemsIds = {}
            fileCounter = 0
            while fileCounter < numItems or nextFile < numItems:
                if nextFile in finishedItemsIds:
                    (inputItem, t) = finishedItemsIds.pop(nextFile)
                    if processItemSequentialMethod != None:
                        processItemSequentialMethod(inputItem, nextFile, numItems)
                    self.logProcessItemInfo(inputItem, t, nextFile, numItems)
                    results.append([inputItem, t])
                    nextFile += 1
                else:
                    [identifier, inputItem, t] = resultQueue.get()
                    finishedItemsIds[identifier] = (inputItem, t)
                    fileCounter += 1
        else:            
            for i in range(numItems):        
                [identifier, inputItem, t] = resultQueue.get()
                if processItemSequentialMethod != None:
                    processItemSequentialMethod(inputItem, identifier, numItems)
                self.logProcessItemInfo(inputItem, t, i, numItems)
                results.append([inputItem, t])
        
        # wait for all children to finish their execution
        for i in range(numProcessesLoad):
            children[i].join()
            
        return results
    
    def runChildLoad(self, childIndex, childrenQueue, resultQueue, processItemParallelMethod):
        kill_received = False
        while not kill_received:
            job = None
            try:
                # This call will patiently wait until new job is available
                job = childrenQueue.get()
            except:
                # if there is an error we will quit the loop
                kill_received = True
            if job == None:
                # If we receive a None job, it means we can stop the grandchildren too
                kill_received = True
            else:            
                [identifier, inputItem] = job
                ti = time.time()
                try:
                    processItemParallelMethod(identifier, inputItem)
                except Exception,e:
                    logging.exception('ERROR loading from ' + inputItem + ':\n' + str(e))
                resultQueue.put([identifier, inputItem, time.time() - ti])
    
    #
    # FOLLOWING METHODS HAVE TO BE IMPLEMENTED BY ALL LOADERS
    #
    def initialize(self):
        """ Initialize the loading procedure """
        raise NotImplementedError( "Should have implemented this" )
     
    def process(self):
        """ Process the input data. Loads/Prepares the data into the system.
        In your implementation we suggest to use processSingle for single process loading
        and processMulti for mulit-process loading"""
        raise NotImplementedError( "Should have implemented this" )
    
    def close(self):
        """ Close the loader procedure """
        raise NotImplementedError( "Should have implemented this" )
    
    def size(self):
        """ Get the size of the data after loading into the system """
        raise NotImplementedError( "Should have implemented this" )
    
    def getNumPoints(self):
        """ Get the total number of loaded points in the system"""
        return 0  
