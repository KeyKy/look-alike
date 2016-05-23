# -*- coding:UTF-8 -*-
# get idf

from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
import os


def runJob(qUserPackageDictPath, outputDir, beginDay, interval_, isForward):
    mconf = MissionConf().setAppName('getQuserPackageToUser')
    msc = MissionContext(conf=mconf)
    msc.getFolder()
    msc.getEmrPyFile()
    cStatus = msc.pySubmit('getQuserPackageToUser', 
                           scriptPath=mconf.getS3ScriptPath, 
                           params=','.join([qUserPackageDictPath,outputDir,beginDay,interval_,isForward]), 
                           taskNodes='4', is_new_cluster='0', clusterName='ykang')
    print cStatus
    msc.stepSleep(cStatus)
    
