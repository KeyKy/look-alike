from com.um.ykang.data.format.File import LineFile

def readFile(one):
    qUsers = []
    f = LineFile().open(srcPath=one, mode='txt', flag='r')
    for line in f:
        qUsers.append(line)
    print len(qUsers)
    print len(set(qUsers))
    f.close()
    return set(qUsers)

def mergeTwoFile(one, two):
    return list(one | two)

def output(one, outputDir):
    f = LineFile().open(outputDir, mode='txt', flag='w')
    for o in one:
        f.writeLine(o)
    f.close()

qUserOne = readFile('/root/look-alike-new/workspace/main/postiveUsers.txt')
qUserTwo = readFile('/root/look-alike-new/workspace/main/totalPostiveUsers.txt')
result = mergeTwoFile(qUserOne, qUserTwo)
output(result, '/root/look-alike-new/workspace/main/totalPostiveUsers2.txt')
