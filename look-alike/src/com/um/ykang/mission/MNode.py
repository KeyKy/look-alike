# -*- coding:UTF-8 -*-

import os

def isUnder(nodePath, parentPath):
    if os.path.isdir(nodePath):
        for (nodename, dirs, files) in os.walk(parentPath):
            if nodename == nodePath:
                return True
        return False
    else:
        for (nodename, dirs, files) in os.walk(parentPath):
            for file_ in files:
                if os.path.join(nodename, file_) == nodePath:
                    return True
        return False