# -*- coding:UTF-8 -*-
import time
import datetime


class TimeUtil(object):
    @staticmethod
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
