# # -*- coding:UTF-8 -*-
# import os
# from com.um.ykang.data.CrossValid import stepSlices
# import re
# 
# def procInterval(dayInterval):
#     return
# 
# basePath = '/root/look-alike/data/qUserOpenPackageInfo'
# # days = sorted(os.listdir(basePath))
# # for dayInterval in stepSlices(days, 7):
# #     print dayInterval
# match_dir = re.compile('/root/look-alike/data/qUserOpenPackageInfo/(2016-01-02|2015-12-27)/.*\.gz')
# for (filename, dirs, files) in os.walk(basePath):
#     #print filename, dirs, files
#     for gzfile in files:
#         if match_dir.search(os.path.join(filename, gzfile)):
#             print os.path.join(filename, gzfile)

# from scipy.sparse import csc_matrix, vstack
# row = [0,1,2]
# col = [0,1,2]
# val = [1,1,1]
# featMat = csc_matrix((val, (row, col)), shape=(3, 3))
# b = csc_matrix(vstack([featMat, featMat]))
# print b.toarray()

# a = {'1':123, '3':12414, '5':3124124}
# for i in a:
#     print i