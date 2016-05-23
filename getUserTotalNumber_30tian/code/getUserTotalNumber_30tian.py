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

def procLine(a):
	output = []
	oneLine = a.strip()
	deviceId_openPackageInfo = oneLine.split('\t')
	
	deviceId = deviceId_openPackageInfo[0]
	records = deviceId_openPackageInfo[1].split('&')
	for record in records:
		deviceType_time_openPackages = record.split('^')
		deviceType = deviceType_time_openPackages[0]
		deviceKey = deviceType + '=' + deviceId
		output.append((deviceKey,1))
	return output

def filterEmpty(a):
	if len(a) == 0:
		return False
	else:
		return True

def addReduce(a,b):
	return a + b
def toStringArr(a):
	return a[0] + '|' + str(a[1])


if __name__ == '__main__':
    sc = SparkContext()

    days = []
    basePath = 's3://datamining.ym/user_profile/last5'
    #outputBasePath = 's3://datamining.ym/dmuser/ykang/results/qUserTotalNumber_2016_01_24_30tian'
    outputBasePath = sys.argv[1]
    for theDay in getDaysGen(sys.argv[2], int(sys.argv[3]), int(sys.argv[4])):
    	days.append(basePath+os.sep+theDay)

    allFolders = ','.join(days)
    last5RDD = sc.textFile(allFolders)
    qUserTotalNumberRDD = last5RDD.flatMap(procLine).filter(filterEmpty)\
    								.reduceByKey(addReduce).map(toStringArr)
    qUserTotalNumberRDD.saveAsTextFile(outputBasePath, 'org.apache.hadoop.io.compress.GzipCodec')
    								



    sc.stop()