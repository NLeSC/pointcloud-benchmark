#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    o.rubi@esciencecenter.nl                                                  #
################################################################################
import os, multiprocessing
from pointcloud.QueriesParameters import QueriesParameters
from pointcloud import utils

class AbstractQuerier:
    """Abstract class for the queriers to be implemented for each different 
    solution for the benchmark."""
    def __init__(self, configuration):
        """ Create the querier, a ConfigParser is required (loaded from a .ini file)"""
        self.conf = configuration
         
    def getConfiguration(self):
        """ Gets the configuration (ConfigParser) instance """
        return self.conf
        
    def run(self):
        config = self.getConfiguration()
        
        # Create and move to the execution path
        executionPath = config.get('General','ExecutionPath')
        if not os.path.isdir(executionPath):
            os.makedirs(executionPath)
        os.chdir(executionPath)    

        # Initialize the PCDMS for querying
        self.initialize()        
        
        # Get which IO devices need to be monitored 
        ioMonitorParam = config.get('General','IOMonitor').strip()
        ioDevices = None
        if ioMonitorParam != '':
            ioDevices = ioMonitorParam.split(',')
        # Get the method to monitor the system usage (CPU / memory)
        # Nothe that all system is monitored (not only the processes related to the queries)
        usageMonitor = config.get('General','UsageMonitor').lower()
        usageMethod = None
        if usageMonitor == 'python':
            usageMethod = utils.getUsagePy
        elif usageMonitor == 'top':
            usageMethod = utils.getUsageTop
        elif usageMonitor == 'ps':
            usageMethod = utils.getUsagePS
        elif usageMonitor != '':
            raise Exception('ERROR: UsageMonitor must be python, ps or top')
        
        # Read the query file 
        queryFileAbsPath = config.get('Query','File')
        queriesParameters = QueriesParameters(queryFileAbsPath)
        
        # Get the identifiers of the several queries
        queryIds = queriesParameters.getIds()
        numQueries = len(queryIds) 
        if numQueries != len(set(queryIds)):
            raise Exception('ERROR: There are duplicated identifiers in given XML')
        
        numUsers = config.getint('Query','NumberUsers')
        numIterations = config.getint('Query','NumberIterations')
        
        # Create queues
        queriesQueue = multiprocessing.Queue() # The queue of tasks (queries)
        resultsQueue = multiprocessing.Queue() # The queue of results
        
        for queryId in queryIds:
            queriesQueue.put(queryId)
        for i in range(numUsers): #we add as many None jobs as numUsers to tell them to terminate (queue is FIFO)
            queriesQueue.put(None)
        
        users = []
        # We start numUsers users processes
        for i in range(numUsers):
            users.append(multiprocessing.Process(target=self.runUser, 
                args=(i, queriesQueue, resultsQueue, numIterations, queriesParameters, usageMethod, ioDevices)))
            users[-1].start()
        
        # We need to receive for each query the two iterations and for each iteration both the results from the query execution and from the monitor 
        numResults = numQueries * numIterations
        
        resultsDict = {}
        for i in range(numResults):
            [userIndex, queryId, iterationId, qTime, qResult, qCPU, qMEM] = resultsQueue.get()
            resultsDict[(queryId, iterationId)] = (userIndex, qTime, qResult, qCPU, qMEM)
        # wait for all users to finish their execution
        for i in range(numUsers):
            users[i].join()
        
        stats = []
        for queryId in queryIds:
            for iterationId in range(numIterations):
                (userIndex, qTime, qResult, qCPU, qMEM) = resultsDict[(queryId, iterationId)]
                qName = str(queryId) + '_' + str(iterationId)
                stats.append((qName, qTime, qResult, qCPU, qMEM))

        self.close()
        return stats
    
    def runUser(self, userIndex, tasksQueue, resultsQueue, numIterations, queriesParameters, usageMethod, ioDevices):
        childResultQueue = multiprocessing.Queue()
        kill_received = False
        while not kill_received:
            queryId = None
            try:
                # This call will patiently wait until new job is available
                queryId = tasksQueue.get()
            except:
                # if there is an error we will quit the generation
                kill_received = True
            if queryId == None:
                # If we receive a None job, it means we can stop this workers 
                # (all the create-image jobs are done)
                kill_received = True
            else:            
                for iterationId in range(numIterations):
                    queryName = queryId + '_' + '_' + str(iterationId)
                    usageAbsPath = os.path.abspath(queryName + '.usage')
                    
                    ioAbsPath = None
                    if ioDevices != None:
                        ioAbsPath = os.path.abspath(queryName + '.io')
                        
                    utils.runMonitor(self.runQuery,(queryId, iterationId, queriesParameters, childResultQueue), usageMethod, usageAbsPath, ioDevices, ioAbsPath)
                    
                    [queryId, iterationId, qTime, qResult] = childResultQueue.get()
                    
                    (times, cpus, mems) = utils.parseUsage(usageAbsPath)
                    (qCPU,qMEM) = (cpus.mean(), mems.mean())
                    imageAbsPath = os.path.abspath(queryName + '_usage.png')
                    utils.saveUsage(times, cpus, mems, queryName + ' CPU/MEM', imageAbsPath)
                    if ioDevices != None:


                        (times, rdata, wdata) = utils.parseIO(ioAbsPath)
                        ioImageAbsPath = os.path.abspath(queryName + '_io.png')
                        utils.saveIO(times, rdata, wdata, queryName + ' IO', ioImageAbsPath)

                    resultsQueue.put((userIndex, queryId, iterationId, qTime, qResult, qCPU, qMEM))   
                    
    def runQuery(self, queryId, iterationId, queriesParameters, resultsQueue):
        try:
            (eTime, result) = self.query(queryId, iterationId, queriesParameters)
            resultsQueue.put((queryId, iterationId, eTime, result))
        except Exception,e:
            print e
            resultsQueue.put((queryId, iterationId, '-', '-'))
    
    #
    # FOLLOWING METHODS HAVE TO BE IMPLEMENTED BY ALL QUERIERS
    #
    def initialize(self):
        """ Initialize the querier procedure """
        raise NotImplementedError( "Should have implemented this" )
    
    def query(self, queryId, iterationId, queriesParameters):
        """ Executes query indicated by queryId. It must return a tuple with (time, results)"""
        raise NotImplementedError( "Should have implemented this" )

    def close(self):
        """ Close the querier procedure"""
        raise NotImplementedError( "Should have implemented this" )