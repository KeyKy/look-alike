import os
from com.um.ykang.mission import MNode
from com.um.ykang.util.BashUtil import BashUtil
import json
import time



class MissionContext:
    def __init__(self, conf):
        self.conf = conf
    
    def __isUnderWorkSpace(self, appPath):
        return MNode.isUnder(appPath, self.conf.getWorkSpace())
    
    def getFolder(self):
        appPath = self.conf.getAppPath()
        if self.__isUnderWorkSpace(appPath):
            return (self, appPath)
        else:
            os.mkdir(appPath)
            return (self, appPath)
    
    def getSample(self, s3SrcPath, headN, recursived=False):
        appPath = self.conf.getAppPath()
        tmpFile = self.conf.getTmpFile()
        if recursived:
            BashUtil.s3Cp(s3SrcPath, appPath, True)
        else:
            srcFilename = os.path.basename(s3SrcPath)
            dstPath = appPath + os.sep + srcFilename
            if os.path.exists(dstPath):
                print 'WRAN: downloading from s3, but file existed'
                BashUtil.subl(tmpFile)
                return self
            else:
                BashUtil.s3Cp(s3SrcPath, dstPath, False)
                [name, ext] = os.path.splitext(srcFilename)
                if ext == '.gz':
                    BashUtil.gzipDecompress(dstPath)
                    BashUtil.head(headN, appPath + os.sep + name, tmpFile)
                    BashUtil.subl(tmpFile)
                elif ext == '.txt':
                    BashUtil.head(headN, dstPath, tmpFile)
                    BashUtil.subl(tmpFile)
                else:
                    BashUtil.head(headN, dstPath, tmpFile)
                    BashUtil.subl(tmpFile)
        return self
    
    def getEmrFile(self):
        codePath = self.conf.getCodePath()
        codeTemplate = self.conf.getCodeTemplate()
        codeFile = self.conf.getCodeFile()
        gitPath = self.conf.getGitPath()
        if os.path.exists(codePath):
            if os.path.exists(codeTemplate):
                BashUtil.move(codeTemplate, codeFile)
            BashUtil.subl(codeFile)
        else:
            BashUtil.gitClone(gitPath, codePath)
            BashUtil.move(codeTemplate, codeFile)
            BashUtil.subl(codeFile)
        return self
    
    def getEmrPyFile(self):
        codePath = self.conf.getCodePath()
        codeTemplate = self.conf.getCodeTemplate()
        codeFile = self.conf.getCodeFile()
        gitPath = self.conf.getSubMitPyGitPath()
        if os.path.exists(codePath):
            if os.path.exists(codeTemplate):
                BashUtil.move(codeTemplate, codeFile)
            BashUtil.subl(codeFile)
        else:
            BashUtil.gitClone(gitPath, codePath)
            BashUtil.move(codeTemplate, codeFile)
            BashUtil.subl(codeFile)
        return self
        
    def pySubmit(self, jobName, scriptPath, params, taskNodes, is_new_cluster='0', clusterName='ykang'):
        BashUtil.s3Cp(self.conf.getCodeFile(), 
                      self.conf.getS3ScriptPath(), 
                      recursived=False)
        cStatus = os.popen(' '.join(['cd', self.conf.getCodePath(), '&&',
                            './add-job.py',
                            '-n', jobName,
                            '-c', 'datamining-cluster'+':'+clusterName,
                            '-f', scriptPath,
                            '-p', params,
                            '-t', taskNodes,
                            '-a', is_new_cluster
                            ])).read()
        print cStatus
        return cStatus
    
    def stepSleep(self, cStatus):
        status = json.loads(cStatus)
        statusInfo = status['info']
        cluster_id = statusInfo['cluster_id']
        step_id = statusInfo['step_id']
        timeout = 1
        while(True):
            step_status = BashUtil.emrStepStatus(cluster_id, step_id)
            step_status = json.loads(step_status)
            step_state = step_status['Steps'][0]['Status']['State']
            if step_state == 'COMPLETED' or step_state == 'FAILED':
                print 'step status %s' % step_state
                return
            if timeout > 480:
                print 'step %s timeout' % step_id
                return
            print 'step %s, timeout %i' % (step_state, timeout)
            timeout += 2
            time.sleep(120)
        
    def submit(self, mem='3G', coreN='4', taskN='3', coreMulti='1'):
        os.system(' '.join(['cd', self.conf.getCodePath(), '&&', \
                             './'+self.conf.getEmrFile(), self.conf.getAppName(),\
                              mem, coreN, taskN, coreMulti]))
