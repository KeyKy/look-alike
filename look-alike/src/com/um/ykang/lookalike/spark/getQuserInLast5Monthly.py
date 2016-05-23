# -*- coding:UTF-8 -*-
# 获取用户打开安装包次数，处理一个月的
# 输入Last5一个月的输出，输出所有属于优质用户的记录

from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext

def runJob(appName, qUserDictPath, outputDir, beginDay, interval_, isForward):
    mconf = MissionConf().setAppName(appName)
    msc = MissionContext(conf=mconf)
    msc.getFolder()
    msc.getEmrPyFile()
    status = msc.pySubmit("getQuserInLast5Monthly", mconf.getS3ScriptPath(), 
                 params=','.join([qUserDictPath,outputDir,beginDay,interval_,isForward]), 
                 taskNodes='4', is_new_cluster='0', clusterName='ykang')
    print status
    msc.stepSleep(status)