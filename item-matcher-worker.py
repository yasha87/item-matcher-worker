import tornado.gen
from nats.io import Client as NATS


import datetime
import gc
import json
import traceback

from match_infra import Logger,ConfigCacheHandlerBase,ConfigurationManager,MessageSender
import uuid





import ItemMatchingManager
from Utils import MLModelExecutor

threadPoolSemaphore = {"availableThreads": -1,"firstSearchId": -1,"firstResultId": -1, "isLocked": True,"isSemaphoreInitialized": False}


class ConfigCacheHandler(ConfigCacheHandlerBase.ConfigCacheHandlerBase):
    def __init__(self,moduleName):
        super(ConfigCacheHandler, self).__init__(moduleName)
        self.templateMergerConfig = {}
        self.configDate = datetime.datetime.strptime("1987-11-15 21:00:00", "%Y-%m-%d %H:%M:%S")

    def getTemplateMergerConfig(self):
        delta = datetime.datetime.now() - self.configDate
        if delta.days>=1:
            self.templateMergerConfig["structuredMatchings"] = self.matchingAPI.getItemMatchPairingDict()
            self.templateMergerConfig["MLModelsRaw"] = self.matchingAPI.getMLModels()[0]
            self.templateMergerConfig["MLModels"] = self.matchingAPI.getMLModels()[1]
            self.configDate = datetime.datetime.now()
        delta = datetime.datetime.now() - self.getMassageConfigDate()
        if delta.days >= 1:
            self.getMessageConfig()
        return self.templateMergerConfig

    def getModelTrainerConfigs(self):
        self.getTemplateMergerConfig()
        return self.templateMergerConfig


class MatchingExecutor:
    def __init__(self):
        self.configCacheHandler = ConfigCacheHandler("template_merging")
        self.configCacheHandler.getTemplateMergerConfig()
        self.logger = Logger.Logger(self.configCacheHandler.getMessageBrokerUrl()
                                    ,self.configCacheHandler.getMessageBrokerPort()
                                    ,self.configCacheHandler.getLogsManagerChannel()
                                    ,self.configCacheHandler.getChannelName()
                                    ,self.configCacheHandler.getLogSchemaName(),False)
        self.mlModelExecutor = MLModelExecutor.MLModelExecutor(self.configCacheHandler.getModelTrainerConfigs()["MLModels"])
        self.var = True

    def setChannelName(self,workerName):
        self.channelName = workerName

    def getLogger(self):
        return self.logger

    def getMessageBrokerUrl(self):
        return self.configCacheHandler.getMessageBrokerUrl()

    def getVar(self):
        return self.var

    def resetVar(self):
        self.var=False

    def getUnstructuredChannelName(self):
        return self.configCacheHandler.getUnstructuredChannelName()

    def getMessageBrokerPort(self):
        return self.configCacheHandler.getMessageBrokerPort()

    def getLogsManagerChannel(self):
        return self.configCacheHandler.getLogsManagerChannel()

    def getChannelName(self):
        return self.channelName

    def getQueueName(self):
        return self.configCacheHandler.getQueueName()

    def initSemaphore(self,searchId,scraperId):
        if threadPoolSemaphore["isSemaphoreInitialized"] == False:
            threadPoolSemaphore["availableThreads"] = self.configCacheHandler.configManager.getTotalThreadLimit()
            threadPoolSemaphore["isSemaphoreInitialized"] = True
            threadPoolSemaphore["firstSearchId"] = searchId
            threadPoolSemaphore["firstScraperId"] = scraperId

    def handleMatchingMessage(self,msg):
        try:


            if msg["commandType"] == "EXECUTE":
                data = {}
                if "data" in msg:
                    data = msg["data"]
                modelingFlag = False
                if 'isModeling' in data:
                    modelingFlag = data["isModeling"]
                processMode, scraperId, uId = None, None, None
                if "data" in msg:
                    if "isLoggerSave" in msg["data"]:
                        msg["data"].pop("isLoggerSave", None)
                    if "processMode" in msg["data"]:
                        processMode = msg["data"]["processMode"]
                    if "scraperId" in msg["data"]:
                        scraperId = msg["data"]["scraperId"]
                    if "uId" in msg["data"]:
                        uId = msg["data"]["uId"]
                self.initSemaphore(msg["searchId"],scraperId)
                self.logger.logMessageRecieved("EXECUTE", msg["searchId"], self.getChannelName(), msg["senderChannel"],data)

                thread = ItemMatchingManager.ItemMatchingManager(msg["searchId"],msg["data"],self.configCacheHandler.getMessageConfig(),self.configCacheHandler.getModelTrainerConfigs()["structuredMatchings"],self.mlModelExecutor,threadPoolSemaphore)
                thread.daemon = True
                thread.start()
                #thread.join()
                gc.collect()

            else:
                self.logger.logEvent(msg["searchId"], "ERROR", "In match-server. Got unknown message type", "Unknown Message command: " + str(msg))
        except Exception, e:
            self.logger.logEvent(msg["searchId"], "ERROR", "match-listener.handleMatchingMessage","Error has accured. Error Message: " + str(e) + ". Trace Back: " + traceback.format_exc())
            self.messageSender = MessageSender.MessageSender(self.configCacheHandler.getMessageBrokerUrl(),
                                                             self.configCacheHandler.getMessageBrokerPort(),
                                                             msg["senderChannel"])
            processMode,scraperId,uId=None,None,None
            msgData = {}
            if "data" in msg:
                if "processMode" in msg["data"]:
                    processMode = msg["data"]["processMode"]
                if "scraperId" in msg["data"]:
                    scraperId = msg["data"]["scraperId"]
                if "uId" in msg["data"]:
                    uId = msg["data"]["uId"]
                if processMode is None and scraperId is None:
                    msgData = {}
                else:
                    msgData = {"processMode": processMode, "scraperId": scraperId, "uId": uId}
            self.messageSender.sendMessageAsync(self.messageSender.createMessageJson("EXECUTE", msg["searchId"],
                                                                                     self.configCacheHandler.getChannelName(), msgData))
            self.logger.logMessageSent("EXECUTE", msg["searchId"], msg["senderChannel"],
                                       self.configCacheHandler.getChannelName(), msgData)
            raise



    def logEvent(self,searchId,event,funcName, message):
        self.logger.logEvent(searchId,event,funcName, message)

matchingExec = MatchingExecutor()
TEMPLATE_MERGER_CHANNEL = matchingExec.configCacheHandler.getChannelName()
matchingExec.setChannelName("Worker_"+str(uuid.uuid4().hex))

import tornado.gen
from nats.io import Client as NATS



@tornado.gen.coroutine
def main():
    nc = NATS()
    options = {"verbose": True,
               "servers": [ConfigurationManager.ConfigurationManager().getMessageUandPwd() + "@"
                           +ConfigurationManager.ConfigurationManager().getMessageBrokerUrl() + ":"
                           +str(ConfigurationManager.ConfigurationManager().getMessageBrokerPort())]}
    yield nc.connect(**options)

    def subscribeMatchServer(msg):
        matchingExec.handleMatchingMessage(json.loads(msg.data))

    yield nc.subscribe(matchingExec.getChannelName(), "job.workers", subscribeMatchServer)


    matchingExec.logEvent(-1,"INFO","item-matcher-worker.main","Started tornado listener on "+matchingExec.getChannelName()+ " channel.")
    matchingExec.logEvent(-1, "INFO", "item-matcher-worker.main",
                          "template-merger on " + TEMPLATE_MERGER_CHANNEL + " channel.")
    messageSender = MessageSender.MessageSender(matchingExec.configCacheHandler.getMessageBrokerUrl(),
                                                matchingExec.configCacheHandler.getMessageBrokerPort(),
                                                matchingExec.configCacheHandler.getChannelName())
    messageSender.sendMessageAsync(messageSender.createMessageJson("SUBSCRIBE", -1,
                                                                   matchingExec.getChannelName(),
                                                                   {}))

if __name__ == '__main__':
    try:
        main()
        tornado.ioloop.IOLoop.current().start()
    except:
        main()
        tornado.ioloop.IOLoop.current().start()