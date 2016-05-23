from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
import os



def runJob(outputDir, beginDay, interval_, isForward):
    mconf = MissionConf().setAppName('getUserTotalNumber_30tian')
    msc = MissionContext(conf=mconf)
    msc.getFolder()
    msc.getEmrPyFile()
    cStatus = msc.pySubmit('getUserTotalNumber_30tian', 
                           scriptPath=mconf.getS3ScriptPath(), 
                           params=','.join([outputDir, beginDay, interval_, isForward]), 
                           taskNodes='4', is_new_cluster='0', clusterName='ykang')
    print cStatus
    msc.stepSleep(cStatus)