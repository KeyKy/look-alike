# -*- coding:UTF-8 -*-
import os
from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
from com.um.ykang.lookalike.model.GBClassifierFeatureFactory import GBClassifierQuserFeature,\
    GBClassifierCandFeature
from scipy.sparse import csc_matrix, vstack
from sklearn import ensemble
from sklearn.externals import joblib
import numpy
from com.um.ykang.data.CrossValid import crossValidSplit, stepSlices
from com.um.ykang.lookalike.entity.Qpackage import Qpackage

class GBClassifierModeling(object):
    
    def __init__(self, appName, qUserPath, candPath, qPackageToId):
        self.mconf = MissionConf().setAppName(appName)
        self.msc = MissionContext(self.mconf)
        [_,self.appPath] = self.msc.getFolder()
        self.qUserPath = qUserPath
        self.candPath = candPath
        self.qPackageToId = qPackageToId
    
    def train(self, neg_pos_factor=4.0/1.0):
        print 'training....'
        qUserFeatureFactory = GBClassifierQuserFeature(self.qUserPath, 100, self.qPackageToId)
        sparseQX, _ = qUserFeatureFactory.cursor.next()
        
        candFeatFactory = GBClassifierCandFeature(self.candPath, int(neg_pos_factor * sparseQX.shape[0]), self.qPackageToId)
        sparseCXNegative, _ = candFeatFactory.cursor.next()

        model = ensemble.GradientBoostingClassifier(max_depth=5)
        postiveTrain = sparseQX
        negativeTrain = sparseCXNegative
        print 'postiveTrain shape', postiveTrain.shape
        print 'negativeTrain shape', negativeTrain.shape
        sparseTrain = csc_matrix(vstack([postiveTrain, negativeTrain]))
        labelTrain = numpy.concatenate([numpy.ones([postiveTrain.shape[0],]), -numpy.ones([negativeTrain.shape[0],])], axis=0)
        model.fit(sparseTrain, labelTrain)
        joblib.dump(model, os.path.join(self.appPath, 'GBClassifier.pkl'))
        
    def predict(self, candPath, num_take, each_take):
        print 'predicting...'
        model = joblib.load(os.path.join(self.appPath, 'GBClassifier.pkl'))
        postiveUsers = []
        candFeatFactory = GBClassifierCandFeature(candPath, each_take, self.qPackageToId)
        for cand, users in candFeatFactory.cursor:
            predictY = model.predict(cand.toarray())
            postiveIdx = [i for i in range(predictY.size) if predictY[i] == 1]
            postiveUsers.extend(([users[i] for i in postiveIdx]))
            print len(postiveUsers)
            if len(postiveUsers) > num_take:
                break
        return postiveUsers
    
    def crossValid(self, neg_pos_factor=4.0/1.0):
        qUserCursor = GBClassifierQuserFeature(self.qUserPath, 100, self.qPackageToId).takeData()
        sparseQX, qUsers = qUserCursor.next()
        
        negativeCursor = GBClassifierCandFeature(self.candPath, int(neg_pos_factor * sparseQX.shape[0]), self.qPackageToId).takeData()
        sparseCXNegative, cnCands = negativeCursor.next()
        
        print sparseQX.shape, sparseCXNegative.shape
        print sparseQX[0,:], sparseCXNegative[0,:]
        print qUsers[0], cnCands[0]
        
        model = ensemble.GradientBoostingClassifier(max_depth=3)
        for (trainSlice, testSlice) in crossValidSplit(sparseQX.shape[0]):
            postiveTrain = sparseQX[trainSlice,:] #tainSlice是训练的rowId
            negativeTrain = sparseCXNegative[0:neg_pos_factor*len(trainSlice),:] #
            
            sparseTrain = csc_matrix(vstack([postiveTrain, negativeTrain]))
            labelTrain = numpy.concatenate([numpy.ones([postiveTrain.shape[0],]), -numpy.ones([negativeTrain.shape[0],])], axis=0)
            
            print 'postiveTrain=%i, negativeTrain=%i' % (postiveTrain.shape[0], negativeTrain.shape[0])
            print '-----------fitting-------------'
            model.fit(sparseTrain, labelTrain)
            
            postiveTest = sparseQX[testSlice,:] #testSlice是测试的rowId
            negativeTest = sparseCXNegative[neg_pos_factor*len(trainSlice):,:] #
            print 'postiveTest=%i, negativeTest=%i' % (postiveTest.shape[0], negativeTest.shape[0])
            
            sparseTest = csc_matrix(vstack([postiveTest, negativeTest]))
            labelTest = numpy.concatenate([numpy.ones([postiveTest.shape[0],]), -numpy.ones([negativeTest.shape[0],])], axis=0)
            
            print '-----------predicting-----------'
            predictY = []
            for slice_ in stepSlices(range(sparseTest.shape[0]), 100):
                y = model.predict(sparseTest[slice_,:].toarray()) #预测必须使用稠密的矩阵，一次性预测会内存溢出
                predictY.extend(list(y))
            predictY = numpy.array(predictY)
            groundTruePostive = len([i for i in range(predictY.size) if predictY[i] == 1 and labelTest[i] == 1])
            groundTrueNegative = len([i for i in range(predictY.size) if predictY[i] == -1 and labelTest[i] == 1])
                   
            postive = len([i for i in range(predictY.size) if predictY[i] == 1])
            negative = len([i for i in range(predictY.size) if predictY[i] == -1])
            print 'groundTruePostive=%i, groundTrueNegative=%i, postive=%i, negative=%i' % (groundTruePostive, groundTrueNegative, postive, negative)
            
            precision = groundTruePostive * 1.0 / (postive)
            recall = groundTruePostive * 1.0 / (groundTrueNegative + groundTruePostive)       
            print 'precision = %f' % precision
            print 'recall = %f' % recall
            
            break #只做了一次
    
if __name__ == '__main__':
    modeling = GBClassifierModeling('justForTest', '/root/look-alike/data/qUserOpenPackageInfo',
                                    '/root/look-alike/data/candidatesInfo', Qpackage.getQpackageToId())
    #modeling.crossValid()
    #modeling.train(4)
    users = modeling.predict('/root/look-alike/data/predict', 1000)
    print users
    