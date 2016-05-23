# -*- coding:UTF-8 -*-
import gzip
from com.um.ykang.data.format.Line import SepLine, RegExpLine, JsonLine
import os

class LineFile(object):
    def __init__(self):
        self._fhandle = None
        self._readOrWrite = None
    
    def open(self, srcPath, mode, flag):
        if flag.count('w') != 0:
            self._readOrWrite = False
        elif flag.count('r') != 0:
            self._readOrWrite = True
        else:
            self._readOrWrite = None
            print "ERROR: unrecognized flag, neither read nor write"
            return self
        
        if self._fhandle != None:
            self._fhandle.close()

        if mode == 'gzip':
            self._fhandle = gzip.open(srcPath, flag)
        elif mode == 'txt':
            self._fhandle = open(srcPath, flag)
        else:
            print "ERROR: unrecognized mode"
        return self
    
    def __iter__(self):
        return self
    
    def next(self):
        if self._readOrWrite == True:
            line = self._fhandle.readline().strip()
            if line != '':
                return line
            else:
                raise StopIteration
        else:
            raise IOError
        
    def close(self):
        self._fhandle.close()
        self._fhandle = None
    
    def writeLine(self, line):
        if self._readOrWrite == False:
            self._fhandle.write(line + os.linesep)
    
    def __del__(self):
        if self._fhandle != None:
            self.close()

class SepFile(LineFile):
    def __init__(self, sep):
        super(SepFile, self).__init__()
        self.sep = sep
        
    def next(self):
        if self._readOrWrite == True:
            line = self._fhandle.readline().strip()
            if line != '':
                return SepLine(line).format(self.sep)
            else:
                raise StopIteration
        else:
            raise IOError

class RegExpFile(LineFile):
    def __init__(self, regExp):
        super(RegExpFile, self).__init__()
        self.__regExp = regExp
    def next(self):
        if self._readOrWrite == True:
            line = self._fhandle.readline().strip()
            if line != '':
                return RegExpLine(line).format(self.__regExp)
            else:
                raise StopIteration
        else:
            raise IOError

class JsonFile(LineFile):
    def __init__(self):
        super(JsonFile, self).__init__()
    
    def next(self):
        if self._readOrWrite == True:
            line = self._fhandle.readline()
            if line != '':
                return JsonLine(line).format()
            else:
                raise StopIteration
        else:
            raise IOError
