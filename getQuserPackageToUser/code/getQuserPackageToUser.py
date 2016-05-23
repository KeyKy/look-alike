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

def qUserOpenPackageDictLine(a):
	spl = a.split('|')
	return {spl[0]:1}

def qUserOpenPackageDictReduce(a,b):
	for bkey in b.keys():
		a[bkey] = 1
	return a

def procLine(a, qUserOpenPackageLookup):
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
			if openPackage in qUserOpenPackageLookup.value:
				output.append((openPackage + '|' + deviceKey, 1))
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

def countOpenPackage(a):
	openPackage_deviceKey = a[0].split('|')
	return (openPackage_deviceKey[0], 1)

def addTimes(a, b):
	return a+b

def toStringArr(a):
	return a[0] + '|' + str(a[1])

def formKey(a):
	openPackage_deviceKey = a[0].split('|')
	return (openPackage_deviceKey[0], openPackage_deviceKey[1] + '|' + str(a[1]))


if __name__ == '__main__':
    sc = SparkContext()
    #qUserOpenPackageRDD = sc.textFile('s3://datamining.ym/dmuser/ykang/data/spark.ouwan.qUserOpenPackage')
    qUserOpenPackageRDD = sc.textFile(sys.argv[1])
    qUserOpenPackageDict = qUserOpenPackageRDD.map(qUserOpenPackageDictLine).reduce(qUserOpenPackageDictReduce)
    qUserOpenPackageLookup = sc.broadcast(qUserOpenPackageDict)

    days = []
    basePath = 's3://datamining.ym/user_profile/last5'
    #outputBasePath = 's3://datamining.ym/dmuser/ykang/results/qUserPackageToUser_2016_01_24_30tian'
    outputBasePath = sys.argv[2]
    #for theDay in getDaysGen('2016-01-24', 30, False):
    for theDay in getDaysGen(sys.argv[3], int(sys.argv[4]), int(sys.argv[5])):
    	days.append(basePath+os.sep+theDay)

    allFolders = ','.join(days)
    last5RDD = sc.textFile(allFolders)

    # qUserPackageToUserRDD = last5RDD.flatMap(lambda a:procLine(a,qUserOpenPackageLookup))\
    # 									.filter(filterEmpty)\
    # 									.reduceByKey(addReduce)\
    # 									.sortBy(formKey)\
    # 									.map(toStringArr)
    qUserPackageToUserRDD = last5RDD.flatMap(lambda a:procLine(a,qUserOpenPackageLookup))\
    									.filter(filterEmpty)\
    									.reduceByKey(addReduce)\
    									.map(countOpenPackage)\
    									.reduceByKey(addTimes)\
    									.map(toStringArr)
    qUserPackageToUserRDD.saveAsTextFile(outputBasePath, 'org.apache.hadoop.io.compress.GzipCodec')

    sc.stop()