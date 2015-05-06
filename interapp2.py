
# coding: utf-8

# In[1]:

import tempfile
import requests
import pandas as pd
import numpy as np
from pandas import Series, DataFrame
import os


# In[2]:

user = 'unicef-hss-spec'
passwd = 'SoonWide29'
BASE_URL = "http://hmis2.health.go.ug/api/analytics.csv?"
BASE_URL = BASE_URL + "dimension=pe:LAST_12_MONTHS&dimension=dx:yTtv6wuTWUN;eGSUL2aL0zW;OWJ3hkJ9VYA;iNVDqc0xKi0"
BASE_URL = BASE_URL + "&dimension=ou:LEVEL-5;%s"
payload = {
    "filter": "u8EjsUj11nz:jTolsq2vJv8;GM7GlqjfGAW;luVzKLwlHJV",
    "tableLayout": "true",
    "columns": "pe;dx",
    "rows": "ou",
}


def get_url(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    return res.text


# In[3]:

csv_download = get_url(BASE_URL % "x75Yh65MaUa")


# In[3]:




# In[4]:

temp = open("download.csv",'r+')
temp.write(csv_download)
temp.close()


# In[5]:

value = pd.read_csv("download.csv")
keys=value.keys()
# computing the number of rows present in the data set, later to be used the ranking
myrows=value.shape[0] -1
myrows = myrows -1
line0=value.values[myrows][0]
check=value.groupby(value['organisationunitname'])
RowToSub={}
for i in range(myrows):
    RowToSub[value.values[i][0]]=value.values[i][1]


# In[6]:

ancList=[1,2,5,6,9,10,13,14,17,18,21,22,25,26,29,30,33,34,37,38,41,42,45,46]
delivList=[3,7,11,15,19,23,27,31,35,39,43,47]
pvcList=[4,8,12,16,20,24,28,32,36,40,44,48]

#The above lists correspond to the column numbers of the data. 
# it is upon the basis that on export the data columns will not change, only the rows will change.
#please note herein that the indices do not take account YET of the seldom used "'Organisation unit'" and 'Organisation unit description

Mydict1={}
Mydict2={}
Mydict={}
WholeTree={}
#herein ANC data is separated
for i in range(myrows):
    Mylist=[]
    for v in ancList:
        v +=3            #This additional line caters for the comment 3 above
        Mylist.append(value.values[i][v])
    Mydict[value.values[i][0]]= Mylist
# Now we will consider the Deliveries in Unit data
for i in range(myrows):
    Mylist=[]
    for v in delivList:
        v +=3
        Mylist.append(value.values[i][v])
    Mydict1[value.values[i][0]]= Mylist
#We'll now consider PVC data
for i in range(myrows):
    Mylist=[]
    for v in pvcList:
        v +=3
        Mylist.append(value.values[i][v])
    Mydict2[value.values[i][0]]= Mylist
# for reporting purposes, we'll have all this data organised into a 
#dictionary of dictionaries, such as to facilitate ease of retrival by key.
WholeTree['ANC']=Mydict
WholeTree['Deliv']=Mydict1
WholeTree['PVC']=Mydict2


# In[7]:

Months={} 
Mappings={}
Mappings['jan']='dec'
Mappings['feb']='jan'
Mappings['mar']='feb'
Mappings['apr']='mar'
Mappings['may']='apr'
Mappings['jun']='may'
Mappings['jul']='jun'
Mappings['aug']='jul'
Mappings['sep']='aug'
Mappings['oct']='sep'
Mappings['nov']='oct'
Mappings['dec']='nov'
#start of Months Dictionary; this is important to maintain due to the need to report given months performace versus perfomrance from past
# period's report
Months['jan']=[10,9,8]
Months['feb']=[11,10,9]
Months['mar']=[0,11,10]
Months['apr']=[0,1,11]
Months['may']=[0,1,2]
Months['jun']=[1,2,3]
Months['jul']=[2,3,4]
Months['aug']=[3,4,5]
Months['sep']=[4,5,6]
Months['oct']=[5,6,7]
Months['nov']=[6,7,8]
Months['dec']=[7,8,9]


# In[8]:

def ANC_report(id,interval):
    "ANC_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g ANC_report('cK5zkZIUFsN','jan')"
    mytype=WholeTree['ANC'][id]
    mynewtype=[]
    x=0
    for i in range(0,len(mytype),2):
        mynewtype.append(mytype[i])
    remapped=np.nan_to_num(mynewtype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average ANC :","for", interval, x/3.00
    #return x/3.00
# start of comparison report bit of the function
    mytypeComp=WholeTree['ANC'][id]
    mynewtypeComp=[]
    xComp=0
    for i in range(0,len(mytypeComp),2):
        mynewtypeComp.append(mytypeComp[i])
    remapped=np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings[interval]]:
        xComp += remapped[i]
    print "Average ANC-Past Report For:", Mappings[interval], xComp/3.00
    #return xComp/3.00
# start of completeness report for ANC
    mytype=WholeTree['ANC'][id]
    mynewtype=[]
    truetest=[]
    x=0
    for i in range(0,len(mytype),2):
        mynewtype.append(mytype[i])
    remapped=np.isnan(mynewtype)
    for i in Months[interval]:
        truetest.append(remapped[i])
    remappednum=truetest.count(False)
    print "Average ANC-Completeness:", (remappednum/3.00)*100,"%"
    #return (remappednum/3.00)*100
def PVC_report(id, interval):
    "PVC_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g PVC_report('cK5zkZIUFsN','jan')"
    mytype=WholeTree['PVC'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average PVC For:", interval, x/3.00
# start of comparison of PVC past Month's report
    mytype=WholeTree['PVC'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    print "Average PVC-Past Report For :", Mappings[interval], x/3.00
# start of comparison of Completeness of PVC past Month's report
    mytype=WholeTree['PVC'][id]
    truetest=[]
    x=0.0000
    remapped=np.isnan(mytype)
    remapped=list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x=truetest.count(False)
    print "Average PVC-Completeness:", (x/3.00)*100,"%"
def Deliv_report(id,interval):
    "Deliv_report(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g Deliv_report('cK5zkZIUFsN','jan')"
    mytype=WholeTree['Deliv'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    print "Average Deliveries For:", interval, x/3.00
# start of comparison for the Past Months Deliveries report
    mytype=WholeTree['Deliv'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    print "Average Deliveries- Past Report For :", Mappings[interval], x/3.00
# start of comparison of Completeness of Deliveries for Month's report
    mytype=WholeTree['Deliv'][id]
    truetest=[]
    x=0.0000
    remapped=np.isnan(mytype)
    remapped=list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x=truetest.count(False)
    print "Average Deliveries-Completeness:", (x/3.00)*100,"%"


# In[9]:

def ord(n):
    return str(int(n))+("th" if 4<=n%100<=20 else {1:"st",2:"nd",3:"rd"}.get(n%10, "th"))


# In[10]:

def CombinedReport(id,interval):
    "CombinedReport(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g CombinedReport('cK5zkZIUFsN','jan')"
    mytype=WholeTree['ANC'][id]
    mynewtype=[]
    x=0
    for i in range(0,len(mytype),2):
        mynewtype.append(mytype[i])
    remapped=np.nan_to_num(mynewtype)
    for i in Months[interval]:
        x += remapped[i]
    A=x/3.00
# start of comparison report bit of the function
    mytypeComp=WholeTree['ANC'][id]
    mynewtypeComp=[]
    xComp=0
    for i in range(0,len(mytypeComp),2):
        mynewtypeComp.append(mytypeComp[i])
    remapped=np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings[interval]]:
        xComp += remapped[i]
    B=xComp/3.00
# start of completeness report for ANC
    mytype=WholeTree['ANC'][id]
    mynewtype=[]
    truetest=[]
    x=0
    for i in range(0,len(mytype),2):
        mynewtype.append(mytype[i])
    remapped=np.isnan(mynewtype)
    for i in Months[interval]:
        truetest.append(remapped[i])
    remappednum=truetest.count(False)
    C=(remappednum/3.00)*100
    mytype=WholeTree['PVC'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    D=x/3.00
# start of comparison of PVC past Month's report
    mytype=WholeTree['PVC'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    E=x/3.00
# start of comparison of Completeness of PVC past Month's report
    mytype=WholeTree['PVC'][id]
    truetest=[]
    x=0.0000
    remapped=np.isnan(mytype)
    remapped=list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x=truetest.count(False)
    F=(x/3.00)*100
    mytype=WholeTree['Deliv'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    G=x/3.00
# start of comparison for the Past Months Deliveries report
    mytype=WholeTree['Deliv'][id]
    x=0
    remapped=np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    H=x/3.00
# start of comparison of Completeness of Deliveries for Month's report
    mytype=WholeTree['Deliv'][id]
    truetest=[]
    x=0.0000
    remapped=np.isnan(mytype)
    remapped=list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x=truetest.count(False)
    I=(x/3.00)*100
    RankDict={}
    RankInitial=[]
    RankInitial.append(ANC_reportRank(id,interval))
    c=RowToSub[id]
    total=0
    for i in check:
        result=check.get_group(c)
    k=result.shape[0]
    for j in range(k):
        p=result.values[j][0]
        RankDict[p]=ANC_reportRank(p,interval)
        total +=1
    del RankDict[id]
    RankList=RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition=Series(RankInitial)
    RankFinal=RankPosition.rank(method='min')[0]
    J=RankFinal
    K=total
    L=ord(J),':of',K
    return A,B,C,D,E,F,G,H,I,L


# In[11]:

def ANC_reportRank(id,interval):
    "ANC_reportRank(id,interval)-- This takes an id and interval interms of shorthand months of the year --E.g ANC_reportRank('cK5zkZIUFsN','jan')"
    mytype=WholeTree['ANC'][id]
    mynewtype=[]
    Q=0
    for i in range(0,len(mytype),2):
        mynewtype.append(mytype[i])
    remapped=np.nan_to_num(mynewtype)
    for i in Months[interval]:
        Q += remapped[i]
    Q=Q/3
    mytype=WholeTree['PVC'][id]
    P=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        P += remapped[i]
    P=P/3
    mytype=WholeTree['Deliv'][id]
    R=0
    remapped=np.nan_to_num(mytype)
    for i in Months[interval]:
        R += remapped[i]
    R=R/3
    return R+Q+P


# In[22]:

def Ranking(a,b):
    RankDict={}
    RankInitial=[]
    RankInitial.append(ANC_reportRank(a,b))
    c=RowToSub[a]
    total=0.00
    for i in check:
        result=check.get_group(c)
    k=result.shape[0]
    for j in range(k):
        p=result.values[j][0]
        RankDict[p]=ANC_reportRank(p,b)
        total +=1
    del RankDict[a]
    RankList=RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition=Series(RankInitial)
    RankFinal=RankPosition.rank(method='min')[0]
    #print 'Ranked:',ord(RankFinal),'on a scale of:', total
    return RankFinal


# In[13]:

ANC_report('cK5zkZIUFsN','may')


# In[14]:

def Report(id,interval):
    ANC_report(id,interval)
    PVC_report(id, interval)
    Deliv_report(id,interval)
    Ranking(id,interval)


# In[25]:

int(Ranking('cK5zkZIUFsN','apr'))


# In[16]:

def GeneralReport(interval):
    "This function takes an interval and returns all the results as turples; E.g run GeneralReport('jan')"
    ReportCard={}
    for i in range(myrows):
        id=value.values[i][0]
        ReportCard[value.values[i][0]]=CombinedReport(id,interval)
    return ReportCard


# In[17]:

IntroText1 = "Sample Introduction: Your facility is selected to receive performance reports.\nEvery month a report will arrive that compares your performance this month to the previous month.+10% means an increase. -10% means a decrease.\n Work to improve your facility every day, we’ll be watching!"
IntroText2 = "Sample Introduction: Your facility is selected to receive performance reports.\nEvery month a report will arrive that compares your performance this month to the previous month. +10% means an increase. -10% means a decrease.\nYou will be compared to other facilities in the subcounty.\nWork to improve your facility every day, we’ll be watching!"
PositiveScore = "Great job, keep up the good work!"
NegativeScore = "You’re slipping behind, work together to improve performance!"
Position = "Work to improve to be #1!"


# In[31]:

def ReportFormat(key,month, group):
    ANCScore = CombinedReport(key,month)[0]
    PCVScore = CombinedReport(key,month)[4]
    DelivScore = CombinedReport(key,month)[7]
    Reporting = ((CombinedReport(key,month)[2] + CombinedReport(key,month)[5] + CombinedReport(key,month)[8]) /300) *100
    TotalScore = ANCScore + PCVScore + DelivScore
    RankPosition = CombinedReport(key,month)[9][0]
    RankPositionOf = CombinedReport(key,month)[9][1]
    RankPositionTot = CombinedReport(key,month)[9][2]
    RankPositionval = int(Ranking(key,month))
    ANCScorePast = CombinedReport(key,month)[1]
    PCVScorePast = CombinedReport(key,month)[5]
    DelivScorePast = CombinedReport(key,month)[8]
    TotalScorePast = ANCScorePast + PCVScorePast + DelivScorePast
    print "Month -{0}: Report-Total Score {1}, ANC1 {2}, DELIVERY {3}, PCV3 {4}".format(month,TotalScore,ANCScore,DelivScore,PCVScore)
    print "Reporting Rate {0}".format(Reporting) + "%"
    if group == 1:
        if TotalScorePast < TotalScore:
            print PositiveScore
        else:
            print NegativeScore
    elif group == 2:
        print "Your facility is currently in {0} place in your subcounty.".format(RankPosition)
        if RankPositionval == 1:
            print PositiveScore
        else:
            print Position
    elif group == 3:
        print "Your facility is currently in {0} place in your subcounty.".format(RankPosition)
        if RankPositionval == 1:
            print PositiveScore
        else:
            print Position
    else:
        pass


# In[35]:

ReportFormat('cxWQMqqkkAF','apr',1)


# In[19]:



