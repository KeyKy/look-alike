# -*- coding:UTF-8 -*-
import random
from itertools import combinations

def stepSlices(rowIdx, stepSize):
    for start in range(0, len(rowIdx), stepSize):
        yield rowIdx[start : start+stepSize]

def rowTag(rowN, sliceN=10, seed=1):
    random.seed(seed) 
    rowIdx = range(rowN) #产生行号
    random.shuffle(rowIdx) #洗牌行号
    eachSliceNum = rowN / sliceN #每份长度
    slices = []
    for i in stepSlices(rowIdx, eachSliceNum): #取得每份list
        slices.append(i)
    
    lenSlices = len(slices)
    if lenSlices > sliceN: #处理多余的最后一份 随机将最后一份的元素平均加入到前面
        belongIdx = random.sample(range(lenSlices-1), len(slices[lenSlices-1])) 
        for i in range(len(belongIdx)):
            slices[belongIdx[i]].append(slices[lenSlices-1][i])
        return slices[0:lenSlices-1]
    return slices


def crossValidSplit(rowN, trainRatio=8, testRatio=2, seed=1):
    slices = rowTag(rowN, trainRatio+testRatio, seed)
    totalIdx = range(trainRatio+testRatio)
    for trainIdx in combinations(totalIdx, trainRatio):
        testIdx = tuple(set(totalIdx) - set(trainIdx))
        trainSlices = reduce(lambda x,y:x+y, [slices[j] for j in trainIdx])
        testSlices = reduce(lambda x,y:x+y, [slices[j] for j in testIdx])
        yield (trainSlices, testSlices)
    