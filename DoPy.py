# coding: utf-8

import pandas as pd
import numpy as np
from pandas import Series

value = pd.read_csv('data1.csv')  # please change this to match the directory of file to use
keys = value.keys()
print keys
# computing the number of rows present in the data set, later to be used the ranking
myrows = value.shape[0]
line0 = value.values[myrows - 1]

# For the following 7 lines of code, we seek to strip the "'Organisation unit'" to identify the subcounty; by which we intend to rank.
mystring = value['Organisation unit']
# print mystring
for i in range(len(mystring)):
    # value['Organisation unit'][i] = value['Organisation unit'][i].split('/')[4]
    value.loc[i, 'Organisation unit'] = value['Organisation unit'][i].split('/')[4]
check = value.groupby(value['Organisation unit'])
RowToSub = {}
for i in range(myrows):
    RowToSub[value.values[i][0]] = value.values[i][1]


ancList = [1, 2, 5, 6, 9, 10, 13, 14, 17, 18, 21, 22, 25, 26, 29, 30, 33, 34, 37, 38, 41, 42, 45, 46]
delivList = [3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47]
pvcList = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48]

# The above lists correspond to the column numbers of the data.
# it is upon the basis that on export the data columns will not change, only the rows will change.
# please note herein that the indices do not take account YET of the seldom used "'Organisation unit'" and 'Organisation unit description

Mydict1 = {}
Mydict2 = {}
Mydict = {}
WholeTree = {}
# herein ANC data is separated
for i in range(myrows):
    Mylist = []
    for v in ancList:
        v += 3  # This additional line caters for the comment 3 above
        Mylist.append(value.values[i][v])
    Mydict[value.values[i][0]] = Mylist
# Now we will consider the Deliveries in Unit data
for i in range(myrows):
    Mylist = []
    for v in delivList:
        v += 3
        Mylist.append(value.values[i][v])
    Mydict1[value.values[i][0]] = Mylist
# We'll now consider PCV data
for i in range(myrows):
    Mylist = []
    for v in pvcList:
        v += 3
        Mylist.append(value.values[i][v])
    Mydict2[value.values[i][0]] = Mylist
# for reporting purposes, we'll have all this data organised into a
# dictionary of dictionaries, such as to facilitate ease of retrival by key.
WholeTree['ANC'] = Mydict
WholeTree['Deliv'] = Mydict1
WholeTree['PCV'] = Mydict2

Mappings = {
    'jan': 'dec',
    'feb': 'jan',
    'mar': 'feb',
    'apr': 'mar',
    'may': 'apr',
    'jun': 'may',
    'jul': 'jun',
    'aug': 'jul',
    'sep': 'aug',
    'oct': 'sep',
    'nov': 'oct',
    'dec': 'nov',
}
# start of Months Dictionary; this is important to maintain due to the need to report given months performace versus perfomrance from past
# period's report
Months = {
    'jan': [10, 9, 8],
    'feb': [11, 10, 9],
    'mar': [0, 11, 10],
    'apr': [0, 1, 11],
    'may': [0, 1, 2],
    'jun': [1, 2, 3],
    'jul': [2, 3, 4],
    'aug': [3, 4, 5],
    'sep': [4, 5, 6],
    'oct': [5, 6, 7],
    'nov': [6, 7, 8],
    'dec': [7, 8, 9],
}


def ANC_report(id, interval):
    "ANC_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g ANC_report('cK5zkZIUFsN','jan')"
    mytype = WholeTree['ANC'][id]
    mynewtype = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average ANC :", "for", interval, x / 3.00
    # return x/3.00
# start of comparison report bit of the function
    mytypeComp = WholeTree['ANC'][id]
    mynewtypeComp = []
    xComp = 0
    for i in range(0, len(mytypeComp), 2):
        mynewtypeComp.append(mytypeComp[i])
    remapped = np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings[interval]]:
        xComp += remapped[i]
    print "Average ANC-Past Report For:", Mappings[interval], xComp / 3.00
    # return xComp/3.00
# start of completeness report for ANC
    mytype = WholeTree['ANC'][id]
    mynewtype = []
    truetest = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.isnan(mynewtype)
    for i in Months[interval]:
        truetest.append(remapped[i])
    remappednum = truetest.count(False)
    print "Average ANC-Completeness:", (remappednum / 3.00) * 100, "%"
    # return (remappednum/3.00)*100


def PCV_report(id, interval):
    "PCV_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g PCV_report('cK5zkZIUFsN','jan')"
    mytype = WholeTree['PCV'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average PCV For:", interval, x / 3.00
# start of comparison of PCV past Month's report
    mytype = WholeTree['PCV'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    print "Average PCV-Past Report For :", Mappings[interval], x / 3.00
# start of comparison of Completeness of PCV past Month's report
    mytype = WholeTree['PCV'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x = truetest.count(False)
    print "Average PCV-Completeness:", (x / 3.00) * 100, "%"


def Deliv_report(id, interval):
    "Deliv_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g Deliv_report('cK5zkZIUFsN','jan')"
    mytype = WholeTree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average Deliveries For:", interval, x / 3.00
# start of comparison for the Past Months Deliveries report
    mytype = WholeTree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    print "Average Deliveries- Past Report For :", Mappings[interval], x / 3.00
# start of comparison of Completeness of Deliveries for Month's report
    mytype = WholeTree['Deliv'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x = truetest.count(False)
    print "Average Deliveries-Completeness:", (x / 3.00) * 100, "%"


def CombinedReport(id, interval):
    "CombinedReport(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g CombinedReport('cK5zkZIUFsN','jan')"
    mytype = WholeTree['ANC'][id]
    mynewtype = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months[interval]:
        x += remapped[i]
    A = x / 3.00
# start of comparison report bit of the function
    mytypeComp = WholeTree['ANC'][id]
    mynewtypeComp = []
    xComp = 0
    for i in range(0, len(mytypeComp), 2):
        mynewtypeComp.append(mytypeComp[i])
    remapped = np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings[interval]]:
        xComp += remapped[i]
    B = xComp / 3.00
# start of completeness report for ANC
    mytype = WholeTree['ANC'][id]
    mynewtype = []
    truetest = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.isnan(mynewtype)
    for i in Months[interval]:
        truetest.append(remapped[i])
    remappednum = truetest.count(False)
    C = (remappednum / 3.00) * 100
    mytype = WholeTree['PCV'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    D = x / 3.00
# start of comparison of PCV past Month's report
    mytype = WholeTree['PCV'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    E = x / 3.00
# start of comparison of Completeness of PCV past Month's report
    mytype = WholeTree['PCV'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x = truetest.count(False)
    F = (x / 3.00) * 100
    mytype = WholeTree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    G = x / 3.00
# start of comparison for the Past Months Deliveries report
    mytype = WholeTree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    H = x / 3.00
# start of comparison of Completeness of Deliveries for Month's report
    mytype = WholeTree['Deliv'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x = truetest.count(False)
    I = (x / 3.00) * 100
    RankDict = {}
    RankInitial = []
    RankInitial.append(ANC_reportRank(id, interval))
    c = RowToSub[id]
    total = 0.00
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, interval)
        total += 1
    del RankDict[id]
    RankList = RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition = Series(RankInitial)
    RankFinal = RankPosition.rank(method='min')[0]
    J = RankFinal
    K = total
    L = J, ':of', K
    return A, B, C, D, E, F, G, H, I, L


def ANC_reportRank(id, interval):
    "ANC_reportRank(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g ANC_reportRank('cK5zkZIUFsN','jan')"
    mytype = WholeTree['ANC'][id]
    mynewtype = []
    Q = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months[interval]:
        Q += remapped[i]
    Q = Q / 3.00
    mytype = WholeTree['PCV'][id]
    P = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        P += remapped[i]
    P = P / 3.00
    mytype = WholeTree['Deliv'][id]
    R = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        R += remapped[i]
    R = R / 3.00
    return R + Q + P


def Ranking(a, b):
    RankDict = {}
    RankInitial = []
    RankInitial.append(ANC_reportRank(a, b))
    c = RowToSub[a]
    total = 0.00
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, b)
        total += 1
    del RankDict[a]
    RankList = RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition = Series(RankInitial)
    RankFinal = RankPosition.rank(method='min')[0]
    print 'Ranked:', RankFinal, 'on a scale of:', total


ANC_report('cK5zkZIUFsN', 'jan')


def Report(id, interval):
    ANC_report(id, interval)
    PCV_report(id, interval)
    Deliv_report(id, interval)
    Ranking(id, interval)

Report('cK5zkZIUFsN', 'jan')


def GeneralReport(interval):
    "This function takes an interval and returns all the results as turples; E.g run GeneralReport('jan')"
    ReportCard = {}
    for i in range(myrows):
        id = value.values[i][0]
        ReportCard[value.values[i][0]] = CombinedReport(id, interval)
    return ReportCard

# GeneralReport('jan')
