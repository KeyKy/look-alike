# -*- coding:UTF-8 -*-
import os
from com.um.ykang.data.CrossValid import stepSlices
import re
from com.um.ykang.data.format.File import SepFile
from scipy.sparse import csc_matrix, vstack
from com.um.ykang.lookalike.entity.Qpackage import Qpackage

class GBClassifierFeature(object):
    def __init__(self, qPackageToId, interval_=7):
        self.interval = interval_
        self.qPackageToId = qPackageToId
    
    def matUserToOpenPackage(self, user2OpenPackage):
        row = []
        col = []
        val = []
        users = []
        idx = 0
        for userName in user2OpenPackage:
            openPackageToTimes = user2OpenPackage[userName]
            users.append(userName)
            for openPackage in openPackageToTimes:
                row.append(idx)
                col.append(self.qPackageToId[openPackage])
                val.append(openPackageToTimes[openPackage] * 1.0 / self.interval)
            idx += 1
        sparseMat = csc_matrix((val, (row, col)), shape=(idx, len(self.qPackageToId)))
        return sparseMat, users

class GBClassifierQuserFeature(GBClassifierFeature):
    
    def __init__(self, qUserPath, takeWeek_, qPackageToId):
        super(GBClassifierQuserFeature, self).__init__(qPackageToId)
        self.qUserPath = qUserPath
        self.takeWeek = takeWeek_
        self.cursor = self.takeData()
    
    def setTakeWeek(self, takeWeek_):
        self.takeWeek = takeWeek_
    
    def genInterval(self):
        days = sorted(os.listdir(self.qUserPath))
        if len(days) % self.interval != 0:
            print 'ERROR: days mod interval not 0'
        for week in stepSlices(days, self.interval):
            yield week
        
    def procInterval(self, week): #quser只能控制周为单位的，不能控制条为单位
        userToOpenPackage = {}
        match_dir = re.compile(os.path.join(self.qUserPath, '(' + '|'.join(week) + ')', '.*\.gz'))
        for (filename, _, files) in os.walk(self.qUserPath):
            for gzfile in files:
                gzfile_dir = os.path.join(filename, gzfile)
                if match_dir.search(gzfile_dir):
                    f = SepFile('|').open(gzfile_dir, mode='gzip', flag='rb')
                    for line in f: #line[0] userId, line[1] packageName, line[2] openTimes
                        if line[0] not in userToOpenPackage:
                            userToOpenPackage[line[0]] = {}
                            if line[1] in self.qPackageToId:
                                if line[1] not in userToOpenPackage[line[0]]:
                                    userToOpenPackage[line[0]][line[1]] = int(line[2])
                                else:
                                    userToOpenPackage[line[0]][line[1]] += int(line[2])
                        else:
                            if line[1] in self.qPackageToId:
                                if line[1] not in userToOpenPackage[line[0]]:
                                    userToOpenPackage[line[0]][line[1]] = int(line[2])
                                else:
                                    userToOpenPackage[line[0]][line[1]] += int(line[2])
                    f.close()
        return userToOpenPackage
    
    def takeData(self):
        currentWeek = 0
        featMat = csc_matrix((1, len(self.qPackageToId)))
        users = []
        for week in self.genInterval():
            tmp = self.procInterval(week)
            matTmp, userTmp = self.matUserToOpenPackage(tmp)
            users.extend(userTmp)
            featMat = csc_matrix(vstack([featMat, matTmp]))
            currentWeek += 1
            if currentWeek >= self.takeWeek:
                yield featMat[1:featMat.shape[0], :]
                featMat = csc_matrix((1, len(self.qPackageToId)))
                currentWeek = 0
                users = []
        yield featMat[1:featMat.shape[0], :], users

class GBClassifierCandFeature(GBClassifierFeature):
    def __init__(self, candPath, takeNum_, qPackageToId):
        super(GBClassifierCandFeature, self).__init__(qPackageToId)
        self.candPath = candPath
        self.takeNum = takeNum_
        self.max_record = int(self.takeNum * 0.1)
        if self.max_record < 1:
            self.max_record = 1
        self.cursor = self.takeData()
    
    def setTakeNum(self, takeNum_):
        self.takeNum = takeNum_
        self.max_record = int(self.takeNum * 0.1)
        if self.max_record < 1:
            self.max_record = 1
    
    def genInterval(self):
        weeks = sorted(os.listdir(self.candPath))
        for weekInterval in stepSlices(weeks, 1):
            yield weekInterval
    
    def procInterval(self, weekInterval): #与Quser不一样因为我们不需要把7天的加起来
        userToOpenPackage = {}
        num_record = 0
        match_dir = re.compile(os.path.join(self.candPath, '(' + '|'.join(weekInterval) + ')', '.*\.gz'))
        for (filename, _, files) in os.walk(self.candPath):
            for gzfile in files:
                gzfile_dir = os.path.join(filename, gzfile)
                if match_dir.search(gzfile_dir):
                    f = SepFile('|').open(gzfile_dir, mode='gzip', flag='rb')
                    for line in f:
                        if line[0] not in userToOpenPackage:
                            if num_record >= self.max_record:
                                yield userToOpenPackage
                                userToOpenPackage = {}
                                userToOpenPackage[line[0]] = {}
                                if line[1] in self.qPackageToId:
                                    userToOpenPackage[line[0]][line[1]] = int(line[2])
                                num_record = 1
                            else:
                                userToOpenPackage[line[0]] = {}
                                if line[1] in self.qPackageToId:
                                    userToOpenPackage[line[0]][line[1]] = int(line[2])
                                num_record += 1
                        else:
                            if line[1] in self.qPackageToId:
                                if line[1] not in userToOpenPackage[line[0]]:
                                    userToOpenPackage[line[0]][line[1]] = int(line[2])
                                else:
                                    userToOpenPackage[line[0]][line[1]] += int(line[2])
                    f.close()
        yield userToOpenPackage
    
    def takeData(self):
        currentNum = 0
        featMat = csc_matrix((1, len(self.qPackageToId)))
        users = []
        for weekInterval in self.genInterval():
            for tmp in self.procInterval(weekInterval):
                matTmp, userTmp = self.matUserToOpenPackage(tmp)
                featMat = csc_matrix(vstack([featMat, matTmp])) #用mat的话不用担心下一个weekInterval和上一个weekInterval存在重复的数据
                users.extend(userTmp)
                currentNum += len(tmp)
                if currentNum >= self.takeNum:
                    yield featMat[1:featMat.shape[0], :], users
                    featMat = csc_matrix((1, len(self.qPackageToId)))
                    users = []
                    currentNum = 0
        yield featMat[1:featMat.shape[0], :], users
        
# if __name__ == '__main__':
#     candFeaturing = GBClassifierCandFeature('/root/look-alike/data/test', 5, Qpackage.getQpackageToId(), None)
#     a = candFeaturing.takeData()
#     c, d = a.next()
#     print c
#     print d
#     candFeaturing.setTakeNum(2)
#     e, f = a.next()
#     print e
#     print f
    
    