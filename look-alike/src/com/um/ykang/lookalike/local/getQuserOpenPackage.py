# -*- coding:UTF-8 -*-
# 将getQuserInLast5Monthly的输出下载到本地作为输入， 输出qUserPackage
from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext

import os
import time
import datetime
from com.um.ykang.util.BashUtil import BashUtil
from com.um.ykang.data.format.File import SepFile, LineFile
def getDaysGen(start, margin, isForward):
    start = time.strptime(start, '%Y-%m-%d')
    startStamp = int(time.mktime(start))
    dateTime = datetime.datetime.fromtimestamp(startStamp)
    for i in range(margin):
        if isForward:
            theDay = dateTime + datetime.timedelta(days=i)
        else:
            theDay = dateTime - datetime.timedelta(days=i)
        theDayTimeStr = theDay.strftime("%Y-%m-%d")
        yield theDayTimeStr

def getQuserOpenPackage(basePath='s3://datamining.ym/dmuser/ykang/results/qUserInLast5EachDay', 
                        beginDay='2016-01-24', 
                        interval_='30',
                        isForward='0',
                        s3DictBasePath='s3://datamining.ym/dmuser/ykang/data/spark.ouwan.qUserOpenPackage',
                        isDownload=True):
    mconf = MissionConf().setAppName('getQuserOpenPackage')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder()
    if isDownload:
        for theDay in getDaysGen(beginDay, int(interval_), int(isForward)):
            BashUtil.s3Cp(os.path.join(basePath,theDay), appPath+os.sep+theDay, recursived=True)
    openPackage = {}
    mask = {'imei=333333333333333':1, 'imei=123456789abcdef':1, 'imei=111111111111111':1, 'imei=012345678912345':1, 'imei=000000000000000':1, 'imei=00000000000000':1}
    for (filename, _, files) in os.walk(appPath):
        print filename
        for gzfile in files:
            [_, ext] = os.path.splitext(gzfile)
            if ext == '.gz':
                f = SepFile('|')
                f.open(filename+os.sep+gzfile, mode='gzip', flag='rb')
                for line in f:
                    if line[0] not in mask:
                        if line[1] not in openPackage:
                            openPackage[line[1]] = int(line[2])
                        else:
                            openPackage[line[1]] += int(line[2])
                f.close()
    openTimes = []
    print 'sorting'
    packs = openPackage.keys()
    for key in packs:
        openTimes.append(openPackage[key])
    index = sorted(range(len(openTimes)), key=lambda k: openTimes[k], reverse=True)
    print 'sorted'
 
    writer = LineFile()
    writer.open(os.path.join(appPath, 'qUserOpenPackage.txt'), mode='txt', flag='w')
    for i in index:
        key = packs[i]
        value = openPackage[key]
        writer.writeLine(key + '|' + str(value))
    writer.close()
    
    BashUtil.s3Cp(os.path.join(appPath, 'qUserOpenPackage.txt'), dst=os.path.join(s3DictBasePath, 'qUserOpenPackage.txt'), recursived=False)
    return openPackage

if __name__ == '__main__':
    mconf = MissionConf().setAppName('getQuserOpenPackage')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder()
    
    basePath = 's3://datamining.ym/dmuser/ykang/results/qUserInLast5EachDay'

    for theDay in getDaysGen('2016-01-24', 30, 0):
        BashUtil.s3Cp(basePath+os.sep+theDay, appPath+os.sep+theDay, recursived=True)
    openPackage = {}
    mask = {'imei=333333333333333':1, 'imei=123456789abcdef':1, 'imei=111111111111111':1, 'imei=012345678912345':1, 'imei=000000000000000':1, 'imei=00000000000000':1}
    #mask = {}
    for (filename, dirs, files) in os.walk(appPath):
        print filename
        for gzfile in files:
            [name, ext] = os.path.splitext(gzfile)
            if ext == '.gz':
                f = SepFile('|')
                f.open(filename+os.sep+gzfile, mode='gzip', flag='rb')
                for line in f:
                    if line[0] not in mask:
                        if line[1] not in openPackage:
                            openPackage[line[1]] = int(line[2])
                        else:
                            openPackage[line[1]] += int(line[2])
                f.close()
    openTimes = []
    print 'sorting'
    packs = openPackage.keys()
    for key in packs:
        openTimes.append(openPackage[key])
    index = sorted(range(len(openTimes)), key=lambda k: openTimes[k], reverse=True)
    print 'sorted'
 
    writer = LineFile()
    writer.open(appPath+os.sep+'qUserOpenPackage.txt', mode='txt', flag='w')
    for i in index:
        key = packs[i]
        value = openPackage[key]
        writer.writeLine(key + '|' + str(value))
    writer.close()
    
    #BashUtil.s3Cp(appPath+os.sep+'qUserOpenPackage.txt', dst='s3://datamining.ym/dmuser/ykang/data/spark.ouwan.qUserOpenPackage/qUserOpenPackage.txt', recursived=False)
    
    
    
    
    