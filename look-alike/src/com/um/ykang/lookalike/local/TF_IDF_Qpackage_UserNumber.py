# -*- coding:UTF-8 -*-
# 使用类似TF-IDF计算每个qUserOpenPackage的重要性,这里用的是30天打开每个安装包的用户个数

from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
from com.um.ykang.util.BashUtil import BashUtil
import os
from com.um.ykang.data.format.File import SepFile, LineFile
import math
import shutil
from com.um.ykang.lookalike.entity.Qpackage import Qpackage

def getQpackageToOpenTimes(appPath): #idf
    qPackageToOpenTimes = {}
    for (filename, _, files) in os.walk(appPath):
        for gzfile in files:
            print gzfile
            [_, ext] = os.path.splitext(gzfile)
            if ext == '.gz':
                reader = SepFile('|')
                reader.open(os.path.join(filename, gzfile), mode='gzip', flag='rb')
                for line in reader:
                    qPackageToOpenTimes[line[0]] = int(line[1])
                reader.close()
    return qPackageToOpenTimes
    
def tf_idf(appPath, qUserOpenPackageToOpenTimes, qPackageToOpenTimes, totalUsers=52180470):
    intersectPackages = set(qUserOpenPackageToOpenTimes.keys()) & (set(qPackageToOpenTimes.keys()))
    writer = LineFile()
    fwriter = LineFile()
    writer.open(os.path.join(appPath,'qUserOpenPackageToImportance.txt'), mode='txt', flag='w')
    fwriter.open(os.path.join(appPath,'qPackageToScore.txt'), mode='txt', flag='w')
    qUserOpenPackage = []
    tfs = []
    userNum = []
    idfs = []
    tf_idfs = []
    log_tf_idfs = []
    linear_tf_idfs = []
    for i in intersectPackages:
        tf = qUserOpenPackageToOpenTimes[i]
        tfs.append(tf)
        
        userNum.append(qPackageToOpenTimes[i])
        
        idf = totalUsers * 1.0 / (qPackageToOpenTimes[i]+1)
        idfs.append(idf)
        
        tf_idf = tf * idf
        tf_idfs.append(tf_idf)
        
        log_tf_idf = (math.log(float(tf))+1) * math.log(idf)
        log_tf_idfs.append(log_tf_idf)
        
        linear_tf_idf = tf * math.log(idf)
        linear_tf_idfs.append(linear_tf_idf)
        
        qUserOpenPackage.append(i)
    
    importance = log_tf_idfs
    index = sorted(range(len(importance)), key=lambda k: importance[k], reverse=True)
    
    for idx in index:
        output = [qUserOpenPackage[idx],  str(tfs[idx]), str(userNum[idx]), str(idfs[idx]), str(tf_idfs[idx]), str(log_tf_idfs[idx]), str(linear_tf_idfs[idx])]
        writer.writeLine('|'.join(output))
        output = [qUserOpenPackage[idx], str(importance[idx])]
        fwriter.writeLine(':'.join(output))
    writer.close()
    fwriter.close()

def main(qUserOpenPackageToOpenTimes,
         qUserPackageToUserS3Path='s3://datamining.ym/dmuser/ykang/results/qUserPackageToUser_2016_01_24_30tian',
         isDownload=True):
    mconf = MissionConf().setAppName('TF_IDF_Qpackage_UserNumber')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder()
    if isDownload:
        BashUtil.s3Cp(qUserPackageToUserS3Path, appPath, recursived=True)
    qPackageToOpenTimes = getQpackageToOpenTimes(appPath)
    #print len(set(qUserOpenPackageToOpenTimes.keys()) - set(qPackageToOpenTimes.keys()))
    #intersectPackages = set(qUserOpenPackageToOpenTimes.keys()) & (set(qPackageToOpenTimes.keys()))
    #print len(intersectPackages)
    tf_idf(appPath, qUserOpenPackageToOpenTimes, qPackageToOpenTimes)
    shutil.copyfile(os.path.join(appPath, 'qPackageToScore.txt'), Qpackage.QPACKAGE_SCORE_TXT)
    
    
    
    
