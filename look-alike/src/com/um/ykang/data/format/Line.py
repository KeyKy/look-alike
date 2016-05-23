# -*- coding:UTF-8 -*-
import re
import ast
import json

class LineString(object):
    def __init__(self, l):
        self.line = l
    
    def format(self):
        return self.line

class RegExpLine(LineString):
    def __init__(self, l):
        super(RegExpLine, self).__init__(l)
    
    def format(self, pattern):
        match = re.findall(pattern, self.line)
        return match

class EvalLine(LineString):
    def __init__(self, l):
        super(EvalLine, self).__init__(l)
    def format(self):
        return ast.literal_eval(self.line)

class SepLine(LineString):
    def __init__(self, l):
        super(SepLine, self).__init__(l)
    def format(self, sep):
        return self.line.split(sep)
    
class JsonLine(LineString):
    def __init__(self, l):
        super(JsonLine, self).__init__(l)
    def format(self):
        return json.loads(self.line)
    
    


