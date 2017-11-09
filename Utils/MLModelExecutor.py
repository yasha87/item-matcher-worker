from sklearn.externals import joblib
from match_infra import MatchingAPI
import os
import traceback

#ModelFiles = [
#    {"businessType":"CONTRIBUTION","modelFileName":"SVM_Model.pkl"}
#    ,{"businessType":"PERSON","modelFileName":"RandomForest_Model.pkl"}
#]


class MLModelExecutor:

    def __init__(self,mlConfigs):
        self.mlConfigs = mlConfigs
        self.matchingAPI = MatchingAPI.MatchingAPI()
        self.classifierDict = {}
        self.loadAllModelFiles()




    def loadAllModelFiles(self):
        for businessTypeName,configRaw in self.mlConfigs.iteritems():
            if businessTypeName not in self.classifierDict:
                self.classifierDict[businessTypeName] = {}
            for scraperId,config in configRaw.iteritems():
                fileName = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/Utils/ModelFiles/" + businessTypeName + "/" + str(scraperId) + "/" + config["modelFile"]
                if config["modelFile"] == 'AutoMatch':
                    self.classifierDict[businessTypeName][scraperId] = 1
                elif os.path.isfile(fileName):
                    self.classifierDict[businessTypeName][scraperId] = {}
                    self.classifierDict[businessTypeName][scraperId] = joblib.load(fileName)  # or ModelFiles/"+modelFileName | ./ModelFiles/"+modelFileName
        self.mlConfigs = self.matchingAPI.getMLModels()[1]


    def getClassifier(self,businessType,scraperId):
        if businessType not in self.classifierDict:
            return None
        elif scraperId not in self.classifierDict[businessType]:
            return None
        else:
            return self.classifierDict[businessType][scraperId]

    def getDefaultValues(self,businessType,scraperId):
        if businessType not in self.mlConfigs:
            return None
        elif scraperId not in  self.mlConfigs[businessType]:
            return None
        else:
            return self.mlConfigs[businessType][scraperId]["defaultValues"]

    def getModelFeatures(self,businessType,scraperId):
        if businessType not in self.mlConfigs:
            return None
        elif scraperId not in  self.mlConfigs[businessType]:
            return None
        else:
            return self.mlConfigs[businessType][scraperId]["modelFeatures"]



    def predictMatchProbability(self,businessType,scraperId,attributeVector):
        classifier = self.getClassifier(businessType,scraperId)
        try:
            if isinstance(classifier,int):#Automatch case
                result = classifier
            else:
                result = classifier.predict(attributeVector)[0]
            return result
        except Exception,e:
            "Error has accured. ProcessMode Error Message: " + str(e) + ". Trace Back: " + traceback.format_exc()
            return -1
