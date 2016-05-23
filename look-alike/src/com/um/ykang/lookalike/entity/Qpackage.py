# -*- coding:UTF-8 -*-
from com.um.ykang.data.format.File import SepFile, LineFile
from com.um.ykang.util.BashUtil import BashUtil

class Qpackage(object):
    QPACKAGE_SCORE_TXT = '/root/look-alike/data/dict/qPackageToScore.txt'
    QPACKAGE_ID_TXT = '/root/look-alike/data/dict/qPackageToId.txt'
    
    @staticmethod
    def writeQpackageToId(mask=None, inOrOut=True):
        f = SepFile(':').open(Qpackage.QPACKAGE_SCORE_TXT, 'txt', 'r')
        idx = 0
        writer = LineFile().open(Qpackage.QPACKAGE_ID_TXT, 'txt', 'w')
        for line in f:
            if mask == None:
                writer.writeLine(line[0] + '|' + str(idx))
                idx += 1
            else:
                if inOrOut:
                    if line[0] in mask:
                        writer.writeLine(line[0] + '|' + str(idx))
                        idx += 1
                else:
                    if line[0] not in mask:
                        writer.writeLine(line[0] + '|' + str(idx))
                        idx += 1
        writer.close()
        f.close()
    
    @staticmethod
    def getQpackageToId(mask=None, inOrOut=True):
        print 'Info: loading QpackageToId'
        qPackageToId = {}
        f = SepFile('|').open(Qpackage.QPACKAGE_ID_TXT, 'txt', 'r')
        for line in f:
            if mask == None:
                qPackageToId[line[0]] = line[1]
            else:
                if inOrOut:
                    if line[0] in mask:
                        qPackageToId[line[0]] = line[1]
                else:
                    if line[0] not in mask:
                        qPackageToId[line[0]] = line[1]
        f.close()
        return qPackageToId
    
    @staticmethod
    def getIdToPackage(mask=None, inOrOut=True):
        idToQpackage = {}
        f = SepFile('|').open(Qpackage.QPACKAGE_ID_TXT, 'txt', 'r')
        for line in f:
            if mask == None:
                idToQpackage[line[1]] = line[0]
            else:
                if inOrOut:
                    if line[0] in mask:
                        idToQpackage[line[1]] = line[0]
                else:
                    if line[0] not in mask:
                        idToQpackage[line[1]] = line[0]
        f.close()
        return idToQpackage
    @staticmethod
    def getPackageToScore(mask=None, inOrOut=True):
        packToScore = {}
        f = SepFile(':')
        f.open(Qpackage.QPACKAGE_SCORE_TXT, mode='txt', flag='r')
        for line in f:
            if mask == None:
                packToScore[line[0]] = float(line[1])
            else:
                if inOrOut:
                    if line[0] in mask:
                        packToScore[line[0]] = float(line[1])
                else:
                    if line[0] not in mask:
                        packToScore[line[0]] = float(line[1])
        f.close()
        return packToScore
    @staticmethod
    def main(qPackageDictPath='s3://datamining.ym/dmuser/ykang/data/spark.ouwan.qPackageDict/qPackageToId.txt'):
        Qpackage.writeQpackageToId()
        BashUtil.s3Cp(Qpackage.QPACKAGE_ID_TXT, qPackageDictPath, recursived=False)
        
        
        
        
        
        
        
        
        
            