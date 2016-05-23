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

def getQuserUsername(a):
	spl = a.split(',')
	username = "imei=" + spl[0]
	if len(spl[0]) == 0 and len(spl[1]) != 0:
		username = "ifa=" + spl[1]
	return {username:1}

def getQuserDict(a,b):
	for bkey in b.keys():
		a[bkey] = 1
	return a

def procLine(a, qUserLookup):
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
		if deviceKey in qUserLookup.value:
			for openPackage in openPackages:
				output.append((deviceKey, openPackage))
		else:
			output.append(('', ''))
	return output

def filterEmpty(a):
	if len(a[0]) == 0:
		return False
	else:
		return True

def crtCom(a):
	return {a:1}
def addValue(a,b):
	if b in a:
		a[b] += 1
	else:
		a[b] = 1
	return a
def comTow(a,b):
    for bkey in b.keys():
        if bkey in a:
            a[bkey] += b[bkey]
        else:
            a[bkey] = b[bkey]
    return a

def toStringArr(a):
	deviceKey = a[0]
	openPackageInfo = a[1]
	output = []
	for key in openPackageInfo.keys():
		output.append(deviceKey + '|' + key + '|' + str(openPackageInfo[key]))
	return os.linesep.join(output)

if __name__ == '__main__':
    sc = SparkContext()
    #payInfoRDD = sc.textFile('s3://datamining.ym/dmuser/ykang/data/spark.ouwan.payQuser')
    payInfoRDD = sc.textFile(sys.argv[1])
    qUserRDD = payInfoRDD.map(getQuserUsername).reduce(getQuserDict)
    qUserLookup = sc.broadcast(qUserRDD)

    basePath = 's3://datamining.ym/user_profile/last5'
    #outputBasePath = 's3://datamining.ym/dmuser/ykang/results/qUserInLast5EachDay'
    outputBasePath = sys.argv[2]
    #for theDay in getDaysGen('2016-01-24', 30, False):
    for theDay in getDaysGen(sys.argv[3], int(sys.argv[4]), int(sys.argv[5])):
    	last5RDD = sc.textFile(basePath+os.sep+theDay)
    	qUserOpenPackageRDD = last5RDD.flatMap(lambda a:procLine(a,qUserLookup))\
    							.filter(filterEmpty)\
    							.combineByKey(crtCom, addValue, comTow)\
    							.map(toStringArr)
    	qUserOpenPackageRDD.saveAsTextFile(outputBasePath+os.sep+theDay, 'org.apache.hadoop.io.compress.GzipCodec')
    sc.stop()