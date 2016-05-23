from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
from com.um.ykang.util.BashUtil import BashUtil
import os
from com.um.ykang.data.format.File import SepFile


def getUserTotalNumber(s3Path, isDownload=True):
    mconf = MissionConf().setAppName('userTotalNumber')
    msc = MissionContext(conf=mconf)
    [self, appPath] = msc.getFolder()
    if isDownload:
        BashUtil.s3Cp(s3Path, appPath, recursived=True)
    userTotalNumber = 0
    for (filename, _, files) in os.walk(appPath):
        for gzfile in files:
            [_, ext] = os.path.splitext(gzfile)
            if ext == '.gz':
                reader = SepFile('|')
                reader.open(os.path.join(filename, gzfile), mode='gzip', flag='rb')
                for line in reader:
                    userTotalNumber += 1
                reader.close()
    return userTotalNumber
    

if __name__ == '__main__':
    mconf = MissionConf().setAppName('userTotalNumber')
    msc = MissionContext(conf=mconf)
    [self, appPath] = msc.getFolder()
    BashUtil.s3Cp('s3://datamining.ym/dmuser/ykang/results/qUserTotalNumber_2016_01_24_30tian', appPath, recursived=True)
    print getUserTotalNumber(appPath)