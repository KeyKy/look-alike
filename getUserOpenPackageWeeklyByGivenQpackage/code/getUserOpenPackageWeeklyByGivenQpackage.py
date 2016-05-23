from pyspark import SparkContext

import os
import time
import datetime
import sys
def getDaysGen(start, margin, isForward):
	start = time.strptime(start, '%Y-%m-%d')
	startStamp = int(time.mktime(start))
	dateTime = datetime.datetime.fromtimestamp(startStamp)
	for i in range(margin):
		if isForward:
			theDay = dateTime + datetime.timedelta(days=i)
		else:
			theDay = dateTime - datetime.timedelta(days=i)
		theDayTimeStr = theDay.strftime("%Y-%m-%d")
		yield theDayTimeStr

def filterEmptyLine(a):
	if len(a.strip()) == 0:
		return False
	else:
		return True

def qPackageDictLine(a):
	oneline = a.strip().split('|')
	return {oneline[0] : 1}

def addLookupReduce(a,b):
	for bkey in b.keys():
		a[bkey] = 1
	return a

def procLine(a, qPackageTFIDFLookup):
	output = []
	oneLine = a.strip()
	deviceId_openPackageInfo = oneLine.split('\t')
	
	deviceId = deviceId_openPackageInfo[0]
	records = deviceId_openPackageInfo[1].split('&')
	for record in records:
		deviceType_time_openPackages = record.split('^')
		deviceType = deviceType_time_openPackages[0]
		openPackages = deviceType_time_openPackages[2].split('|')
		deviceKey = deviceType + '=' + deviceId
		
		for openPackage in openPackages:
			if openPackage in qPackageTFIDFLookup.value:
				output.append((deviceKey+'|'+openPackage, 1))
			else:
				output.append(('', ''))
	return output

def filterEmpty(a):
	if len(a[0]) == 0:
		return False
	else:
		return True

def addReduce(a, b):
	return a+b

def toStringArr(a):
	return a[0] + '|' + str(a[1])

def formKey(a):
	deviceKey_openPackage = a[0].split('|')
	return (deviceKey_openPackage[0], deviceKey_openPackage[1] + '|' + str(a[1]))

if __name__ == '__main__':
    sc = SparkContext()
    
    qPackageTFIDF = sc.textFile(sys.argv[1])
    qPackageTFIDFDict = qPackageTFIDF.filter(filterEmptyLine).map(qPackageDictLine).reduce(addLookupReduce)
    qPackageTFIDFLookup = sc.broadcast(qPackageTFIDFDict)

    days = []
    basePath = 's3://datamining.ym/user_profile/last5'
    
    outputBasePath = sys.argv[2]
    
    for theDay in getDaysGen(sys.argv[3], int(sys.argv[4]), int(sys.argv[5])):
    	days.append(basePath+os.sep+theDay)

    allFolders = ','.join(days)
    last5RDD = sc.textFile(allFolders)
    qUserPackageToUserRDD = last5RDD.flatMap(lambda a:procLine(a,qPackageTFIDFLookup))\
    									.filter(filterEmpty)\
    									.reduceByKey(addReduce)\
    									.sortBy(formKey)\
    									.map(toStringArr)
    qUserPackageToUserRDD.saveAsTextFile(outputBasePath, 'org.apache.hadoop.io.compress.GzipCodec')

    sc.stop()