#-*- coding:UTF-8 -*-
# 得到用户的在Qpackage上的打开包信息
from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
import os

def runJob(qPackageDictPath, outputDir, beginDay, interval_, isForward):
    mconf = MissionConf().setAppName('getUserOpenPackageWeeklyByGivenQpackage')
    msc = MissionContext(conf=mconf)
    msc.getFolder()
    msc.getEmrPyFile()
    status = msc.pySubmit('getUserOpenPackageWeeklyByGivenQpackage', 
                          scriptPath=mconf.getS3ScriptPath(), 
                          params=','.join([qPackageDictPath,outputDir,beginDay,interval_,isForward]), 
                          taskNodes='4', is_new_cluster='0', clusterName='ykang')
    msc.stepSleep(status)
