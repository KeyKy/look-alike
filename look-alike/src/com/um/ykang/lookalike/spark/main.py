# -*- coding:UTF-8 -*-
from com.um.ykang.mission.Constance import Constance
import os
import json
from com.um.ykang.lookalike.spark import getQuserInLast5Monthly,\
    getQuserPackageToUser, getUserTotalNumber,\
    getUserOpenPackageWeeklyByGivenQpackage
from com.um.ykang.lookalike.local.getQuserOpenPackage import getQuserOpenPackage
from com.um.ykang.lookalike.local import userTotalNumber,\
    TF_IDF_Qpackage_UserNumber
from com.um.ykang.lookalike.entity.Qpackage import Qpackage
from com.um.ykang.lookalike.model import GBClassifierModel
from com.um.ykang.lookalike.entity.Candidate import Candidate
from com.um.ykang.lookalike.entity.Quser import Quser
from com.um.ykang.data.format.File import LineFile, SepFile
from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
from com.um.ykang.lookalike.model.GBClassifierModeling import GBClassifierModeling
from com.um.ykang.util.BashUtil import BashUtil

if __name__ == '__main__':
    with open('/root/workspace/look-alike/src/com/um/ykang/lookalike/spark/paramsConf.json') as json_data:
        paramsConf = json.load(json_data)
    BASE_PATH = paramsConf['BASE_PATH']
    DATA_BASE_PATH = os.path.join(BASE_PATH,'data')
    
    payQuserS3Path = paramsConf['payQuser'] #支付用户的S3路径
    qUserInLast5EachDayS3Path = paramsConf['qUserInLast5EachDay'] #从Last5抓取支付用户的行为输出基路径
    tfBeginDay = paramsConf['tfBeginDay'] #计算TF的起始日期
    tfInterval = paramsConf['tfInterval'] #计算TF的日期长度
    tfIsForward = paramsConf['tfIsForward'] #向前还是向后计算
    qUserOpenPackageS3Path = paramsConf['qUserOpenPackage'] #计算TF的结果，S3输出路径
    qUserPackageToUserS3Path = paramsConf['qUserPackageToUser'] #计算IDF，每个包的使用人数
    userTotalNumberS3Path = paramsConf['userTotalNumber'] #计算IDF，总人数
    qPackageDictS3Path = paramsConf['qPackageDict'] #优质包字典的S3路径
    userOpenPackageWeeklyByGivenQpackageS3Path = paramsConf['userOpenPackageWeeklyByGivenQpackage'] #取一周Candidates的输出路径
    candidateBeginDay = paramsConf['candidateBeginDay'] #取Candidates起始日期
    candidateInterval = paramsConf['candidateInterval'] #日期长度 
    candidateIsForward = paramsConf['candidateIsForward'] #向前或者向后
    trainQuserBeginDay = paramsConf['trainQuserBeginDay']
    trainInterval = paramsConf['trainInterval'] #训练集的起始日期
    trainIsForward = paramsConf['trainIsForward'] #训练集的日期长度
    np_factor = float(paramsConf['neg_pos_factor']) #训练集的负正比例
    predictPath = paramsConf['predictPath'] #需预测文件的路径
def setPath():
    Candidate.BASE_PATH = os.path.join(DATA_BASE_PATH, 'candidatesInfo')
    Candidate.CANDIDATES_ID_PICKLE = os.path.join(DATA_BASE_PATH,'dict','candToId.bat')
    Candidate.CANDIDATES_ID_TXT = os.path.join(DATA_BASE_PATH,'dict','candToId.txt')
    #Candidate.PART_FILE_NAME = [os.path.join(Candidate.BASE_PATH,i) for i in os.listdir(Candidate.BASE_PATH)]
    Qpackage.QPACKAGE_SCORE_TXT = os.path.join(DATA_BASE_PATH,'dict','qPackageToScore.txt')
    Qpackage.QPACKAGE_ID_TXT = os.path.join(DATA_BASE_PATH,'dict','qPackageToId.txt')
    Quser.QUSER_ID_TXT = os.path.join(DATA_BASE_PATH,'dict','qUserToId.txt')
    Quser.QUSER_OPENPACKAGE_PATH = os.path.join(DATA_BASE_PATH,'qUserOpenPackageInfo')
    Quser.TOTAL_QUSER_TXT = os.path.join(DATA_BASE_PATH,'payQualityUsers','payQualityUsers.txt')
    
def makeFolder():
    if not os.path.exists(Constance.WORK_SPACE):
        os.makedirs(Constance.WORK_SPACE)
    if not os.path.exists(DATA_BASE_PATH):
        os.makedirs(DATA_BASE_PATH)
        os.mkdir(os.path.join(DATA_BASE_PATH,'dict')) #存放字典目录，qPackage字典的存放路径
        os.mkdir(os.path.join(DATA_BASE_PATH, 'payQualityUsers')) #优质用户存放路径
        os.mkdir(os.path.join(DATA_BASE_PATH, 'candidatesInfo'))
    
if __name__ == '__main__':
    makeFolder()
    setPath()
    mconf = MissionConf().setAppName('main')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder() 
#     BashUtil.s3Cp(Quser.TOTAL_QUSER_TXT, payQuserS3Path, recursived=False)
#     #从last5中计算一个月的优质用户行为
#     getQuserInLast5Monthly.runJob(payQuserS3Path,
#                                   qUserInLast5EachDayS3Path,
#                                   tfBeginDay,tfInterval,tfIsForward)
#  
#     #将getQuserInLast5Monthly的结果下载到本地计算tf，并将优质用户打开的所有包上传到S3
#     qUserOpenPackageToOpenTimes = getQuserOpenPackage(qUserInLast5EachDayS3Path,
#                         tfBeginDay, tfInterval, tfIsForward,
#                         qUserOpenPackageS3Path, isDownload=True)
#  
#     #计算每个包到使用用户个数的字典，用于计算idf
#     getQuserPackageToUser.runJob(qUserOpenPackageS3Path, 
#                                  qUserPackageToUserS3Path, 
#                                  tfBeginDay, tfInterval, tfIsForward)
#     #计算每个用户出现次数
#     getUserTotalNumber.runJob(userTotalNumberS3Path, 
#                               tfBeginDay, tfInterval, tfIsForward)
#     #将结果下载到本地统计总用户
#     userTotalNumber.getUserTotalNumber(userTotalNumberS3Path, isDownload=True)
#  
#     #tf-idf
#     TF_IDF_Qpackage_UserNumber.main(qUserOpenPackageToOpenTimes, qUserPackageToUserS3Path, isDownload=True)
#     #将tf-idf后的字典上传到S3上用于后面过滤，其实qPackage和qUserOpenPackageToOpenTimes在包的总数没区别，这里只是保留为以后筛选优质包所用
#     Qpackage.main(os.path.join(qPackageDictS3Path, 'qPackage.txt'))
#  
#     #获得Candidates一周的数据，用qPackage过滤
#     getUserOpenPackageWeeklyByGivenQpackage.runJob(qPackageDictS3Path, 
#                                                    userOpenPackageWeeklyByGivenQpackageS3Path, 
#                                                    candidateBeginDay, candidateInterval, candidateIsForward)
#  
#     gbModel = GBClassifierModeling('GBCModeling', Quser.QUSER_OPENPACKAGE_PATH, Candidate.BASE_PATH, Qpackage.getQpackageToId())
#     gbModel.train(neg_pos_factor=np_factor)
#     
#     print gbModel.predict(candPath=predictPath, num_take=100, each_take=10)

if __name__ == '__main__': #不用TF-IDF
    setPath() 
    makeFolder()
    mconf = MissionConf().setAppName('main')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder()   
        
#     #从last5中计算一个月的优质用户行为
#     getQuserInLast5Monthly.runJob('getQuserInLast5Last', payQuserS3Path,
#                                 qUserInLast5EachDayS3Path,
#                                 tfBeginDay,tfInterval,tfIsForward)
#     
#     #将getQuserInLast5Monthly的结果下载到本地计算tf，并将优质用户打开的所有包上传到S3
#     qUserOpenPackageToOpenTimes = getQuserOpenPackage(qUserInLast5EachDayS3Path,
#                         tfBeginDay, tfInterval, tfIsForward,
#                         qUserOpenPackageS3Path, isDownload=False)
#     #可以将qUserOpenPackageToOpenTimes写入到该位置Qpackage.QPACKAGE_ID_TXT
#     index = 0; f = LineFile().open(Qpackage.QPACKAGE_ID_TXT, mode='txt', flag='w')
#     for qPackage in qUserOpenPackageToOpenTimes:
#         f.writeLine(qPackage + '|' + str(index))
#         index += 1
#     f.close()
#     
#     #获得Candidates一周的数据，用qPackage过滤
#     getUserOpenPackageWeeklyByGivenQpackage.runJob(qPackageDictS3Path, 
#                                                    userOpenPackageWeeklyByGivenQpackageS3Path, 
#                                                    candidateBeginDay, candidateInterval, candidateIsForward)
#     BashUtil.s3Cp(userOpenPackageWeeklyByGivenQpackageS3Path, 
#                   Candidate.BASE_PATH, recursived=True)
#    
#     gbModel = GBClassifierModeling('GBCModeling', Quser.QUSER_OPENPACKAGE_PATH, Candidate.BASE_PATH, Qpackage.getQpackageToId())
#     #gbModel.train(neg_pos_factor=np_factor)
#     #gbModel.crossValid(np_factor)
# 
#     postiveUser = gbModel.predict(candPath=predictPath, num_take=2000000, each_take=10000)
#     f = LineFile().open(srcPath=os.path.join(appPath, 'postiveUsers.txt'), mode='txt', flag='w')
#     for pUser in postiveUser:
#         f.writeLine(pUser)
#     f.close()
    
    
    