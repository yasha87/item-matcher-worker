import threading
import traceback
import random
from time import sleep
from contextlib import closing

from match_infra import ConfigurationManager,Logger
from multiprocessing.pool import ThreadPool

from ItemMatcher import ItemMatcher

class ItemMatchingManager(threading.Thread):


    def __init__(self,searchId,executionPlan,messageConfigData,structuredMatchings,mlModelExecutor,threadPoolSemaphore):
        super(ItemMatchingManager, self).__init__()
        self.searchId = searchId
        self.executionPlan = executionPlan
        self.messageConfigData = messageConfigData
        self.mlModelExecutor = mlModelExecutor
        self.structuredMatchings = structuredMatchings
        self.threadPoolSemaphore = threadPoolSemaphore
        self.innerThreadNum = ConfigurationManager.ConfigurationManager().getThreadNum()
        self.logger = Logger.Logger(self.messageConfigData["messageSenderConfig"]["brokerUrl"]
                                    , self.messageConfigData["messageSenderConfig"]["brokerPort"]
                                    , self.messageConfigData["messageSenderConfig"]["logsManagerChannel"]
                                    , self.messageConfigData["messageSenderConfig"]["channel"]
                                    , self.messageConfigData["messageSenderConfig"]["logSchemaName"])


    def handleException(self,e):
        self.logger.logEvent(self.searchId, "ERROR", "ItemMatchingManager.run", "Error has occurred. Error Message: " + str(
            e) + ". Trace Back: " + traceback.format_exc())



    def run(self):
        try:
            while self.threadPoolSemaphore["isLocked"] == True or self.threadPoolSemaphore["availableThreads"] == 0:
                if self.threadPoolSemaphore["firstSearchId"] == self.searchId:
                    self.threadPoolSemaphore["isLocked"] = False
                    break
                sleep(random.randrange(0, 1) * 5)
                # this line is meant to eliminate race conditions
            self.threadPoolSemaphore["availableThreads"] -= self.innerThreadNum
            self.handleItemMerging()
            self.logger.logEvent(self.searchId, "INFO", "ItemMatchingManager.run",
                                 "Finished ItemResult merging execution.")
            self.threadPoolSemaphore["availableThreads"] += self.innerThreadNum

        except Exception, e:
            self.handleException(e)
            self.threadPoolSemaphore["availableThreads"] += self.innerThreadNum

    def initEndpointAccess(self):

        self.logger = Logger.Logger(self.messageConfigData["messageSenderConfig"]["brokerUrl"]
                                    , self.messageConfigData["messageSenderConfig"]["brokerPort"]
                                    , self.messageConfigData["messageSenderConfig"]["logsManagerChannel"]
                                    , self.messageConfigData["messageSenderConfig"]["channel"]
                                    , self.messageConfigData["messageSenderConfig"]["logSchemaName"]
                                    , isInnerThread = False)
        #the child threads should use "add_callback" (not "run_sync") and utilize MatchingManager's Tornado.IOloop
        self.messageConfigData["messageSenderConfig"]["isNotMainThread"] = True

    def handleItemMerging(self):
        itemMatchers=[]
        for itemResultType,execDict in self.executionPlan.iteritems():
            for pair in execDict["itemResultPairs"]:
                itemMatchers.append(ItemMatcher(self.structuredMatchings,self.mlModelExecutor,self.messageConfigData,pair,execDict["itemResults"][str(pair[0])],execDict["itemResults"][str(pair[1])],itemResultType))

        with closing(ThreadPool(ConfigurationManager.ConfigurationManager().getThreadNum()*2)) as pool:
            pool.map(lambda r: r.calcMatchScore(), itemMatchers)