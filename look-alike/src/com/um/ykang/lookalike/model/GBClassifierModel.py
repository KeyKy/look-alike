# # -*- coding:UTF-8 -*-
# from com.um.ykang.lookalike.entity.Qpackage import Qpackage
# import time
# import datetime
# from com.um.ykang.util.BashUtil import BashUtil
# from scipy.sparse import csc_matrix, vstack
# import os
# from com.um.ykang.mission.MissionConf import MissionConf
# from com.um.ykang.mission.MissionContext import MissionContext
# from com.um.ykang.lookalike.entity.Quser import Quser
# from com.um.ykang.data.CrossValid import stepSlices, crossValidSplit
# from com.um.ykang.data.format.File import SepFile
# from com.um.ykang.lookalike.entity.Candidate import Candidate
# import numpy
# from sklearn import ensemble
# from sklearn.externals import joblib
# def getDaysGen(start, margin, isForward):
#     start = time.strptime(start, '%Y-%m-%d')
#     startStamp = int(time.mktime(start))
#     dateTime = datetime.datetime.fromtimestamp(startStamp)
#     for i in range(margin):
#         if isForward:
#             theDay = dateTime + datetime.timedelta(days=i)
#         else:
#             theDay = dateTime - datetime.timedelta(days=i)
#         theDayTimeStr = theDay.strftime("%Y-%m-%d")
#         yield theDayTimeStr
# 
# class GBClassifier(object):
#     def __init__(self):
#         self.model = None
#     
#     @staticmethod
#     def getData(basePath='s3://datamining.ym/dmuser/ykang/results/qUserInLast5EachDay',
#                 beginDay='2015-12-26', interval_='35', isForward='1',
#                 candS3Path='s3://datamining.ym/dmuser/ykang/results/userOpenPackageWeeklyByGivenQpackage_2016_01_30_7tian'):
#         for theDay in getDaysGen(beginDay, int(interval_), int(isForward)):
#             BashUtil.s3Cp(os.path.join(basePath,theDay), os.path.join(Quser.QUSER_OPENPACKAGE_PATH, theDay), recursived=True)
#         BashUtil.s3Cp(candS3Path, Candidate.BASE_PATH, recursived=True)
#     
#     @staticmethod
#     def sparseQX(qUserPath, qPackageToId, packToScore):
#         interval = 7
#         def procInterval(dayInterval):
#             userToOpenPackage = {}
#             for theDay in dayInterval:
#                 filePath = os.path.join(qUserPath, theDay)
#                 files = os.listdir(filePath)
#                 for gzfile in files:
#                     [_, ext] = os.path.splitext(gzfile)
#                     if ext == '.gz':
#                         f = SepFile('|')
#                         f.open(os.path.join(filePath, gzfile), mode='gzip', flag='rb')
#                         for line in f:
#                             if line[0] not in userToOpenPackage:
#                                 userToOpenPackage[line[0]] = {}
#                                 if line[1] in qPackageToId:
#                                     if line[1] not in userToOpenPackage[line[0]]:
#                                         userToOpenPackage[line[0]][line[1]] = int(line[2])
#                                     else:
#                                         userToOpenPackage[line[0]][line[1]] += int(line[2])
#                             else:
#                                 if line[1] in qPackageToId:
#                                     if line[1] not in userToOpenPackage[line[0]]:
#                                         userToOpenPackage[line[0]][line[1]] = int(line[2])
#                                     else:
#                                         userToOpenPackage[line[0]][line[1]] += int(line[2])
#                         f.close()                
#             return userToOpenPackage
#         
#         def matInterval(user2OpenPackage):
#             row = []
#             col = []
#             val = []
#             users = []
#             idx = 0
#             for userName in user2OpenPackage.keys():
#                 openPackageToTimes = user2OpenPackage[userName]
#                 users.append(userName)
#                 for openPackage in openPackageToTimes.keys():
# #                     row.append(idx)
#                     col.append(qPackageToId[openPackage])
#                     val.append(openPackageToTimes[openPackage] * 1.0 / interval)
#                     #val.append(packToScore[openPackage] * openPackageToTimes[openPackage] * 1.0 / interval)
#                     #val.append(packToScore[openPackage])
#                 idx += 1
#             sparseMat = csc_matrix((val, (row, col)), shape=(idx, len(qPackageToId.keys())))
#             
#             return sparseMat, users
#         
#         days = sorted(os.listdir(qUserPath))
#         sparseQX = csc_matrix((1, len(qPackageToId.keys())))
#         for dayInterval in stepSlices(days, interval):
#             userToOpenPackage = procInterval(dayInterval)
#             sparseMatTmp, _ = matInterval(userToOpenPackage)
#             sparseQX = csc_matrix(vstack([sparseQX, sparseMatTmp]))
#             #print sparseMatTmp[0,:], users[0], userToOpenPackage[users[0]], sparseMatTmp.shape
#             #print sparseQX[1,:], sparseQX.shape        
#             #break
#         return sparseQX[1:sparseQX.shape[0], :]    
#     @staticmethod
#     def sparseCXNegative(candPartPath, qPackageToId, max_record, packToScore):
#         interval = 7
#         def procMaxRecord():
#             userToOpenPackage = {}
#             files = os.listdir(candPartPath)
#             num_records = 0
#             for gzfile in files:
#                 [_, ext] = os.path.splitext(gzfile)
#                 if ext == '.gz':
#                     f = SepFile('|')
#                     f.open(os.path.join(candPartPath, gzfile), mode='gzip', flag='rb')
#                     for line in f:
#                         if line[0] not in userToOpenPackage:
#                             if num_records >= max_record:
#                                 return userToOpenPackage
#                             else:
#                                 userToOpenPackage[line[0]] = {}
#                                 if line[1] in qPackageToId:
#                                     userToOpenPackage[line[0]][line[1]] = int(line[2])
#                                 num_records += 1
#                         else:
#                             if line[1] in qPackageToId:
#                                 if line[1] not in userToOpenPackage[line[0]]:
#                                     userToOpenPackage[line[0]][line[1]] = int(line[2])
#                                 else:
#                                     userToOpenPackage[line[0]][line[1]] += int(line[2])
#                     f.close()
#         def matMaxRecord(user2OpenPackage):
#             row = []
#             col = []
#             val = []
#             users = []
#             idx = 0
#             for userName in user2OpenPackage.keys():
#                 openPackageToTimes = user2OpenPackage[userName]
#                 users.append(userName)
#                 for openPackage in openPackageToTimes.keys():
#                     row.append(idx)
#                     col.append(qPackageToId[openPackage])
#                     val.append(openPackageToTimes[openPackage] * 1.0 / interval)
#                     #val.append(packToScore[openPackage] * openPackageToTimes[openPackage] * 1.0 / interval)
#                     #val.append(packToScore[openPackage])
#                 idx += 1
#             sparseMat = csc_matrix((val, (row, col)), shape=(idx, len(qPackageToId.keys())))
#             return sparseMat, users
#         
#         userToOpenPackage = procMaxRecord()
#         sparseMatTmp, _ = matMaxRecord(userToOpenPackage)
#         return sparseMatTmp
#     
#     def crossValid(self, sparseQX, sparseCXNegative, neg_pos_factor):
#         self.model = ensemble.GradientBoostingClassifier(max_depth=5)
#         
#         for (trainSlice, testSlice) in crossValidSplit(sparseQX.shape[0]):
#             postiveTrain = sparseQX[trainSlice,:]
#             negativeTrain = sparseCXNegative[0:neg_pos_factor*len(trainSlice),:]
#                  
#             sparseTrain = csc_matrix(vstack([postiveTrain, negativeTrain]))
#             labelTrain = numpy.concatenate([numpy.ones([postiveTrain.shape[0],]), -numpy.ones([negativeTrain.shape[0],])], axis=0)
#                  
#             print 'postiveTrain=%i, negativeTrain=%i' % (postiveTrain.shape[0], negativeTrain.shape[0])
#             print '-----------fitting-------------'
#             self.model.fit(sparseTrain, labelTrain)
#             #joblib.dump(self.model, os.path.join(appPath, 'max_depth_5','GBDT.pkl'))
# #---------------------------------------------------------------------------------------------------------
# 
#             postiveTest = sparseQX[testSlice,:]
#             negativeTest = sparseCXNegative[neg_pos_factor*len(trainSlice):,:]
#             print 'postiveTest=%i, negativeTest=%i' % (postiveTest.shape[0], negativeTest.shape[0])
#                   
#             #self.model = joblib.load(os.path.join(appPath,'max_depth_5|tfidf|noOpenTimes', 'GBDT.pkl'))
#                   
#             sparseTest = csc_matrix(vstack([postiveTest, negativeTest]))
#             labelTest = numpy.concatenate([numpy.ones([postiveTest.shape[0],]), -numpy.ones([negativeTest.shape[0],])], axis=0)
#                  
#             print '-----------predicting-----------'
#             predictY = []
#             for slice_ in stepSlices(range(sparseTest.shape[0]), 100):
#                 y = self.model.predict(sparseTest[slice_,:].toarray()) #预测必须使用稠密的矩阵，一次性预测会内存溢出
#                 predictY.extend(list(y))
#                   
#             predictY = numpy.array(predictY)
#             groundTruePostive = len([i for i in range(predictY.size) if predictY[i] == 1 and labelTest[i] == 1])
#             groundTrueNegative = len([i for i in range(predictY.size) if predictY[i] == -1 and labelTest[i] == 1])
#                   
#             postive = len([i for i in range(predictY.size) if predictY[i] == 1])
#             negative = len([i for i in range(predictY.size) if predictY[i] == -1])
#             print 'groundTruePostive=%i, groundTrueNegative=%i, postive=%i, negative=%i' % (groundTruePostive, groundTrueNegative, postive, negative)
#             precision = groundTruePostive * 1.0 / (postive)
#             recall = groundTruePostive * 1.0 / (groundTrueNegative + groundTruePostive)
#                   
#             print 'precision = %f' % precision
#             print 'recall = %f' % recall
# # 
#             break #只做了一次
#         
#         return self
#     def train(self, sparseQX, sparseCXNegative, path):
#         self.model = ensemble.GradientBoostingClassifier(max_depth=5)
#         postiveTrain = sparseQX
#         negativeTrain = sparseCXNegative
#         
#         sparseTrain = csc_matrix(vstack([postiveTrain, negativeTrain]))
#         labelTrain = numpy.concatenate([numpy.ones([postiveTrain.shape[0],]), -numpy.ones([negativeTrain.shape[0],])], axis=0)
#         
#         self.model.fit(sparseTrain, labelTrain)
#         joblib.dump(self.model, path)
#     
# def mainTrain(qUserS3Path, beginDay, interval_, isForward, candS3Path, neg_pos_factor=4.0 / 1.0, isDownload=True, showCrossValid=True):
#     mconf = MissionConf().setAppName('GBClassifierModeling')
#     msc = MissionContext(conf=mconf)
#     [_,appPath] = msc.getFolder()
#     qPackageToId = Qpackage.getQpackageToId()
#     packToScore = Qpackage.getPackageToScore()
#     if isDownload:
#         GBClassifier.getData(qUserS3Path, beginDay, interval_, isForward, candS3Path)
#     
#     sparseQX = GBClassifier.sparseQX(Quser.QUSER_OPENPACKAGE_PATH, qPackageToId, packToScore)
#     sparseCXNegative = GBClassifier.sparseCXNegative(Candidate.BASE_PATH, qPackageToId, int(sparseQX.shape[0] * neg_pos_factor), packToScore)
#     model1 = GBClassifier()
#     if showCrossValid:
#         model1.crossValid(sparseQX, sparseCXNegative, neg_pos_factor)
#     model1.train(sparseQX, sparseCXNegative, path=os.path.join(appPath, 'GBClassifier.pkl'))
# 
# def mainPredict(maxQuser=100, max_record=100, interval = 7):
#     mconf = MissionConf().setAppName('GBClassifierModeling')
#     msc = MissionContext(conf=mconf)
#     [_,appPath] = msc.getFolder()
#     qPackageToId = Qpackage.getQpackageToId()
#     #packToScore = Qpackage.getPackageToScore()
#     model1 = GBClassifier()
#     model1.model = joblib.load(os.path.join(appPath, 'GBClassifier.pkl'))
#     quser = []
#     def genCandidate(candPartPath, max_record):
#         userToOpenPackage = {}
#         files = os.listdir(candPartPath)
#         files.reverse()
#         num_record = 0
#         for gzfile in files:
#             [_, ext] = os.path.splitext(gzfile)
#             if ext == '.gz':
#                 f = SepFile('|')
#                 f.open(os.path.join(candPartPath, gzfile), mode='gzip', flag='rb')
#                 for line in f:
#                     if line[0] not in userToOpenPackage:
#                         if num_record >= max_record:
#                             yield userToOpenPackage
#                             userToOpenPackage = {}
#                             num_record = 0
#                         else:
#                             userToOpenPackage[line[0]] = {}
#                             if line[1] in qPackageToId:
#                                 userToOpenPackage[line[0]][line[1]] = int(line[2])
#                             num_record += 1
#                     else:
#                         if line[1] in qPackageToId:
#                             if line[1] not in userToOpenPackage[line[0]]:
#                                 userToOpenPackage[line[0]][line[1]] = int(line[2])
#                             else:
#                                 userToOpenPackage[line[0]][line[1]] += int(line[2])
#                 f.close()
#     def matCandidate(user2OpenPackage):
#         row = []
#         col = []
#         val = []
#         users = []
#         idx = 0
#         for userName in user2OpenPackage.keys():
#             openPackageToTimes = user2OpenPackage[userName]
#             users.append(userName)
#             for openPackage in openPackageToTimes.keys():
#                 row.append(idx)
#                 col.append(qPackageToId[openPackage])
#                 val.append(openPackageToTimes[openPackage] * 1.0 / interval)
#             idx += 1
#         sparseMat = csc_matrix((val, (row, col)), shape=(idx, len(qPackageToId.keys())))
#         return sparseMat, users
#     candGen = genCandidate(Candidate.BASE_PATH, max_record)
#     #postiveY = []
#     for user2OpenPackage in candGen:
#         sparseCX, users = matCandidate(user2OpenPackage)
#         #print sparseCX[0,:], users[0], sparseCX.shape
#         predictY = model1.model.predict(sparseCX.toarray())
#         postiveIdx = [i for i in range(predictY.size) if predictY[i] == 1]
#         #postiveY.extend([predictY[i] for i in postiveIdx])
#         quser.extend([users[i] for i in postiveIdx])
#         if len(quser) > maxQuser:
#             break
#     return quser
#     #print postiveY
#         
# if __name__ == '__main__':
#     mainPredict()
# #     mconf = MissionConf().setAppName('test')
# #     msc = MissionContext(conf=mconf)
# #     [_,appPath] = msc.getFolder()
# #     neg_pos_factor = 1.0 / 4.0
# #     
# #     qPackageToId = Qpackage.getQpackageToId()
# #     packToScore = Qpackage.getPackageToScore()
#     #print qPackageToId.keys()[0], qPackageToId[qPackageToId.keys()[0]]
#     #print packToScore.keys()[0], packToScore[packToScore.keys()[0]]
# #     basePath = 's3://datamining.ym/dmuser/ykang/results/qUserInLast5EachDay'
# #     for theDay in getDaysGen('2015-12-26', 35, True):
# #         BashUtil.s3Cp(os.path.join(basePath,theDay), os.path.join(Quser.QUSER_OPENPACKAGE_PATH, theDay), recursived=True)
# #     
#     #sparseQX = GBClassifier.sparseQX(Quser.QUSER_OPENPACKAGE_PATH, qPackageToId, packToScore)
#     #print sparseQX[0,:]
# 
#     #BashUtil.s3Cp('s3://datamining.ym/dmuser/ykang/results/userOpenPackageWeeklyByGivenQpackage_2016_01_30_7tian', Candidate.BASE_PATH, recursived=True)
#     #sparseCXNegative = GBClassifier.sparseCXNegative(Candidate.BASE_PATH, qPackageToId, int(sparseQX.shape[0] / neg_pos_factor), packToScore)
#     #print sparseCXNegative.shape
#     
#     #model1 = GBClassifier()
#     #model1.crossValid(sparseQX, sparseCXNegative, 1.0 / neg_pos_factor)
#     #model1.train(sparseQX, sparseCXNegative, path=os.path.join(appPath, 'GBDTClassifier.pkl'))
