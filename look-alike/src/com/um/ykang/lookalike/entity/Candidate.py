import os
from com.um.ykang.data.format.File import SepFile, LineFile
import gc
import cPickle as pickle
import re

class Candidate(object):
    BASE_PATH = '/root/look-alike/data/candidatesInfo'
    CANDIDATES_ID_PICKLE = '/root/proj/look-alike/data/dict/candToId.bat'
    CANDIDATES_ID_TXT = '/root/proj/look-alike/data/dict/candToId.txt'
    
    #PART_FILE_NAME = [os.path.join(BASE_PATH,i) for i in os.listdir(BASE_PATH)]
    
    @staticmethod
    def writeCandidateToId():
        candidates = set()
        match_dir = re.compile(os.path.join(Candidate.BASE_PATH, '.*\.gz'))
        for (filename, _, files) in os.walk(Candidate.BASE_PATH):
            for gzfile in files:
                gzfile_dir = os.path.join(filename, gzfile)
                if match_dir.search(gzfile_dir):
                    f = SepFile('|').open(gzfile_dir, mode='gzip', flag='r')
                    for line in f:
                        candidates.add(line[0])
                    f.close()
        candidates = list(candidates)
        writer = LineFile().open(Candidate.CANDIDATES_ID_TXT, mode='txt', flag='w')
        candToId = {}
        for i in range(len(candidates)):
            candToId[candidates[i]] = str(i)
            writer.writeLine(candidates[i] + '|' + str(i))
        writer.close()

        del candidates
        gc.collect()
        pickle.dump(candToId, open(Candidate.CANDIDATES_ID_PICKLE, 'wb'), True)
        
#     @staticmethod
#     def writeCandidateToId():
#         print 'Info: Writing canditateToId'
#         candidate = set()
#         for pf in Candidate.PART_FILE_NAME:
#             print 'Info: processing ' + pf
#             f = SepFile('|').open(pf, 'gzip', 'rb')
#             for line in f:
#                 candidate.add(line[0])
#             f.close()
#         candidate = list(candidate)
#         writer = LineFile()
#         writer.open(Candidate.CANDIDATES_ID_TXT, 'txt', 'w')
#         candToId = {}
#         for i in range(len(candidate)):
#             candToId[candidate[i]] = str(i)
#             writer.writeLine(candidate[i] + '|' + str(i))
#         writer.close()
#         
#         del candidate
#         gc.collect()
#         pickle.dump(candToId, open(Candidate.CANDIDATES_ID_PICKLE, 'wb'), True)
    
    @staticmethod
    def getCandidateToId(mask=None):
        print 'Info: Loading canditateToId'
        if mask == None:
            candToId = pickle.load(open(Candidate.CANDIDATES_ID_PICKLE, 'rb'))
        else:
            candToId = {}
            f = SepFile('|').open(Candidate.CANDIDATES_ID_TXT, 'txt', 'r')
            for line in f:
                if line[0] in mask:
                    candToId[line[0]] = line[1]
            f.close()
        return candToId
    
    @staticmethod
    def getIdToCandidate(mask=None):
        print 'Info: Loading idToCandidate'
        idToCand = {}
        f = SepFile('|').open(Candidate.CANDIDATES_ID_TXT, 'txt', 'r')
        for line in f:
            if mask == None:
                idToCand[line[1]] = line[0]
            else:
                if line[1] in mask:
                    idToCand[line[1]] = line[0]
        f.close()
        return idToCand

if __name__ == '__main__':
    Candidate.writeCandidateToId()
        