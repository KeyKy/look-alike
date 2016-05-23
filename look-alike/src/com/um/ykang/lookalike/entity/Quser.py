# -*- coding:UTF-8 -*-
from com.um.ykang.data.format.File import SepFile, LineFile

class Quser(object):
    QUSER_ID_TXT = '/root/proj/look-alike/data/dict/qUserToId.txt'
    TOTAL_QUSER_TXT = '/root/proj/look-alike/data/payQualityUsers/payQualityUsers.txt'
    QUSER_OPENPACKAGE_PATH = '/root/proj/look-alike/data/qCustomerOpenPackageInfo'
    
    @staticmethod
    def writeQuserToId(mask=None, inOrOut=True):
        qUser = set()
        f = SepFile(',').open(Quser.TOTAL_QUSER_TXT, 'txt', 'r')
        for line in f:
            username = 'imei=' + line[0]
            if len(line[0]) == 0 and len(line[1]) != 0:
                username = 'idfa=' + line[1]
            if mask == None:
                qUser.add(username)
            else:
                if inOrOut:
                    if username in mask:
                        qUser.add(username)
                else:
                    if username not in mask:
                        qUser.add(username)
        qUser = list(qUser)
        f.close()
        
        f = LineFile().open(Quser.QUSER_ID_TXT, 'txt', 'w')
        for i in range(len(qUser)):
            f.writeLine(qUser[i] + '|' + str(i))
        f.close()
        
    @staticmethod
    def getQuserToId(mask=None, inOrOut=True):
        qUserToId = {}
        f = SepFile('|').open(Quser.QUSER_ID_TXT, 'txt', 'r')
        for line in f:
            if mask == None:
                qUserToId[line[0]] = line[1]
            else:
                if inOrOut:
                    if line[0] in mask:
                        qUserToId[line[0]] = line[1]
                else:
                    if line[0] not in mask:
                        qUserToId[line[0]] = line[1]
        f.close()
        return qUserToId
    
    @staticmethod
    def getIdToQuser(mask=None, inOrOut=True):
        idToQuser = {}
        f = SepFile('|').open(Quser.QUSER_ID_TXT, 'txt', 'r')
        for line in f:
            if mask == None:
                idToQuser[line[1]] = line[0]
            else:
                if inOrOut:
                    if line[1] in mask:
                        idToQuser[line[1]] = line[0]
                else:
                    if line[1] not in mask:
                        idToQuser[line[1]] = line[0]
        f.close()
        return idToQuser
if __name__ == '__main__':
    mask = {'imei=333333333333333':1, 'imei=123456789abcdef':1, 'imei=111111111111111':1, 'imei=012345678912345':1, 'imei=000000000000000':1, 'imei=00000000000000':1}
    Quser.writeQuserToId(mask, False)

        