from match_infra import MatchingAPI, ValueMatcher, ValueAggregator,MessageSender,Logger
import traceback

class ItemMatcher:
    def __init__(self,structuredMatchings,mlModelExecutor,messagingConfig,itemResultPair,itemResult1,itemResult2,itemType):
        self.structuredMatchings = structuredMatchings
        self.matchingApi = MatchingAPI.MatchingAPI()
        self.valueMatcher = ValueMatcher.ValueMatcher(mlModelExecutor)
        self.valueAggregator = ValueAggregator.ValueAggregator()
        self.messagingConfig = messagingConfig["messageSenderConfig"]
        self.itemResultPair = itemResultPair
        self.itemResult1 = itemResult1
        self.itemResult2 = itemResult2
        self.itemType = itemType
        self.logger = Logger.Logger(self.messagingConfig["brokerUrl"]
                                    , self.messagingConfig["brokerPort"]
                                    , self.messagingConfig["logsManagerChannel"]
                                    , self.messagingConfig["channel"]
                                    , self.messagingConfig["logSchemaName"])



    def calcMatchScore(self):
        try:
            self.logger.logEvent(self.itemResult1["searchId"], "INFO", "ItemMatcher.calcMatchScore",
                                 "Statrted ItemResultMatching on " + str(self.itemResult1["_id"])+" and "+ str(self.itemResult2["_id"]))
            if self.itemType not in self.structuredMatchings:
                return False
            scoreDict = self.initScoreDict(self.itemResult1, self.itemResult2, self.itemType)
            itemResultFlat1 = self.matchingApi.flatten_json(self.itemResult1)
            itemResultFlat2 = self.matchingApi.flatten_json(self.itemResult2)


            if "userFeedbackLabel" in itemResultFlat1 and "userFeedbackLabel" in itemResultFlat2:
                if itemResultFlat1["userFeedbackLabel"] == itemResultFlat2["userFeedbackLabel"] and itemResultFlat1["userFeedbackLabel"]!=-1:
                    scoreDict["userFeedbackLabel"] = 1
                else:
                    scoreDict["userFeedbackLabel"] = 0
            for pairingRow in self.structuredMatchings[self.itemType]:
                iptFieldsContent = self.valueMatcher.getConcatenatedValues(itemResultFlat1, pairingRow["itemResultField"])
                outFieldContent = self.valueMatcher.getConcatenatedValues(itemResultFlat2, pairingRow["itemResultField"])
                if outFieldContent is not None and iptFieldsContent is not None and (((isinstance(outFieldContent,
                                                                                                  list) and all(
                                e is not None and len(e) > 0 for e in outFieldContent)) or outFieldContent is not None) and \
                                                                                             ((isinstance(outFieldContent,
                                                                                                          list) and all(
                                                                                                         e is not None and len(
                                                                                                     e) > 0 for e in
                                                                                                         iptFieldsContent)) or iptFieldsContent is not None)):
                    iptScoreArray = []
                    singleScore = []
                    for inputVal in iptFieldsContent:
                        outputScoresArray = []
                        for outputVal in outFieldContent:
                            if inputVal in [None, {}, ''] or outputVal in [None, {}, '']:
                                continue
                            dictScoreArray = self.valueMatcher.getOutputVsInputScore(inputVal, outputVal, pairingRow["method"],
                                                                                     pairingRow["itemField"])
                            outputScoresArray.append(dictScoreArray)

                        if not all(len(e) == 0 for e in outputScoresArray):
                            if len(outputScoresArray) > 1:
                                singleScore = self.valueAggregator.getSingleScoreByFormula(outputScoresArray,
                                                                                           pairingRow[
                                                                                               "outputHeuristicsFormulas"])
                            else:
                                singleScore = outputScoresArray[0]
                            iptScoreArray.append(singleScore)
                    if len(singleScore) > 0 and len(iptScoreArray) > 0:
                        matchPairingDict = {"matchPairingId": pairingRow["attributeId"],
                                            "matchScores": self.valueAggregator.getSingleScoreByFormula(iptScoreArray,
                                                                                                        pairingRow[
                                                                                                            "inputHeuristicsFormulas"])}
                        scoreDict["matchPairingOutputs"].append(matchPairingDict)
            self.matchingApi.insertResultsMatchPairingScore(self.valueMatcher.calcFinalMatchingScore(scoreDict))
            self.sendMessageWithScore(itemResultFlat1["searchId"],self.itemResultPair,scoreDict["matchedScore"])
            if "matchedScore" in scoreDict and scoreDict["matchedScore"]==1:
                return True
            else:
                return False
        except Exception,e:
            self.logger.logEvent(self.itemResult1["searchId"], "ERROR", "ItemMatcher.calcMatchScore",
                                 "Error has occurred. Error Message: " + str(
                                     e) + ". Trace Back: " + traceback.format_exc())
            self.sendMessageWithScore(self.itemResult1["searchId"], self.itemResultPair, 0)

    def initScoreDict(self,itemResult1,itemResult2,itemType):
        fromItems = itemResult1["fromItems"]
        fromItems.extend(itemResult2["fromItems"])
        scoreDict = {"searchId":itemResult1["searchId"],
                        "type": itemType,
                     "fromScraper": 0,
                     "itemIds":fromItems,
                     "matchPairingOutputs": []}
        return scoreDict

    def sendMessageWithScore(self,searchId,itemResultPair,score):

        messageSender = MessageSender.MessageSender(self.messagingConfig["brokerUrl"],
                                                    self.messagingConfig["brokerPort"],
                                                    self.messagingConfig["channel"])
        messageSender.sendMessageAsync(messageSender.createMessageJson("FINISH", searchId,
                                                                       self.messagingConfig["channel"],
                                                                       {"pair":itemResultPair,"score":score}))


