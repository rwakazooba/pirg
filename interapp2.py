import tempfile
import requests
import pandas
import numpy as np
import os
from pandas import Series
from settings import config
import datetime
import psycopg2
import psycopg2.extras
import json

user = config['dhis2_user']
passwd = config['dhis2_passwd']
BASE_URL = config['base_url']
BASE_URL = BASE_URL + "dimension=pe:LAST_12_MONTHS&dimension=dx:yTtv6wuTWUN;eGSUL2aL0zW;OWJ3hkJ9VYA;iNVDqc0xKi0"
BASE_URL = BASE_URL + "&dimension=ou:LEVEL-5;%s"
payload = {
    "filter": "u8EjsUj11nz:jTolsq2vJv8;GM7GlqjfGAW;luVzKLwlHJV",
    "tableLayout": "true",
    "columns": "pe;dx",
    "rows": "ou",
}

# the month for which we're generating the reports
current_month = datetime.datetime.now().strftime('%B').lower()[:3]

ancList = [1, 2, 5, 6, 9, 10, 13, 14, 17, 18, 21, 22, 25, 26, 29, 30, 33, 34, 37, 38, 41, 42, 45, 46]
delivList = [3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47]
pvcList = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48]

Months = {}
Mappings = {}
Mappings['jan'] = 'dec'
Mappings['feb'] = 'jan'
Mappings['mar'] = 'feb'
Mappings['apr'] = 'mar'
Mappings['may'] = 'apr'
Mappings['jun'] = 'may'
Mappings['jul'] = 'jun'
Mappings['aug'] = 'jul'
Mappings['sep'] = 'aug'
Mappings['oct'] = 'sep'
Mappings['nov'] = 'oct'
Mappings['dec'] = 'nov'
# start of Months Dictionary; this is important to maintain due to the need to report given months performace versus perfomrance from past
# period's report
Months['jan'] = [10, 9, 8]
Months['feb'] = [11, 10, 9]
Months['mar'] = [0, 11, 10]
Months['apr'] = [0, 1, 11]
Months['may'] = [0, 1, 2]
Months['jun'] = [1, 2, 3]
Months['jul'] = [2, 3, 4]
Months['aug'] = [3, 4, 5]
Months['sep'] = [4, 5, 6]
Months['oct'] = [5, 6, 7]
Months['nov'] = [6, 7, 8]
Months['dec'] = [7, 8, 9]


def get_url(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    return res.text


def read_csv_to_file(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    f = tempfile.NamedTemporaryFile(delete=False)  # create temporary file
    fname = f.name
    for chunck in res.iter_content(1024):
        f.write(chunck)
    f.close()
    pobj = pandas.read_csv(fname)
    os.unlink(fname)
    return pobj


def send_facility_sms(msg, params):  # params has the facility uuid and other params
    res = requests.get(config["smsurl"], params=params, auth=(config["smsuser"], config["smspasswd"]))
    return res.text


def read_csv_to_file2(url):
    pobj = pandas.read_csv("/tmp/2015-05-12_35_211431419721.csv")
    return pobj


def ord(n):
    return str(int(n)) + ("th" if 4 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def CombinedReport(id, interval, tree):
    """CombinedReport(id,interval)-- This takes an id and interval interms of shorthand
    months of the year --E.g CombinedReport('cK5zkZIUFsN','jan')"""
    mytype = tree['ANC'][id]
    mynewtype = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months[interval]:
        x += remapped[i]
    A = x / 3.00
# start of comparison report bit of the function
    mytypeComp = tree['ANC'][id]
    mynewtypeComp = []
    xComp = 0
    for i in range(0, len(mytypeComp), 2):
        mynewtypeComp.append(mytypeComp[i])
    remapped = np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings[interval]]:
        xComp += remapped[i]
    B = xComp / 3.00
# start of completeness report for ANC
    mytype = tree['ANC'][id]
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
    mytype = tree['PVC'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    D = x / 3.00
# start of comparison of PVC past Month's report
    mytype = tree['PVC'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    E = x / 3.00
# start of comparison of Completeness of PVC past Month's report
    mytype = tree['PVC'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months[interval]:
        truetest.append(remapped[i])
    x = truetest.count(False)
    F = (x / 3.00) * 100
    mytype = tree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        x += remapped[i]
    G = x / 3.00
# start of comparison for the Past Months Deliveries report
    mytype = tree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings[interval]]:
        x += remapped[i]
    H = x / 3.00
# start of comparison of Completeness of Deliveries for Month's report
    mytype = tree['Deliv'][id]
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
    RankInitial.append(ANC_reportRank(id, interval, tree))
    c = RowToSub[id]
    total = 0
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, interval, tree)
        total += 1
    del RankDict[id]
    RankList = RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition = Series(RankInitial)
    RankFinal = RankPosition.rank(method='min')[0]
    J = RankFinal
    K = total
    L = ord(J), ':of', K
    return A, B, C, D, E, F, G, H, I, L


def ANC_reportRank(id, interval, tree):
    """ANC_reportRank(id,interval)-- This takes an id and interval interms of shorthand "
    months of the year --E.g ANC_reportRank('cK5zkZIUFsN','jan')"""
    mytype = tree['ANC'][id]
    mynewtype = []
    Q = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months[interval]:
        Q += remapped[i]
    Q = Q / 3
    mytype = tree['PVC'][id]
    P = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        P += remapped[i]
    P = P / 3
    mytype = tree['Deliv'][id]
    R = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[interval]:
        R += remapped[i]
    R = R / 3
    return R + Q + P

IntroText1 = (
    "Sample Introduction: Your facility is selected to receive performance reports."
    "\nEvery month a report will arrive that compares your performance this month to the previous "
    "month.+10% means an increase. -10% means a decrease.\n Work to improve your facility everyday, "
    "we'll be watching!")
IntroText2 = (
    "Sample Introduction: Your facility is selected to receive performance reports."
    "\nEvery month a report will arrive that compares your performance this month to the previous"
    " month. +10% means an increase. -10% means a decrease.\nYou will be compared to other facilities"
    " in the subcounty.\nWork to improve your facility every day, we'll be watching!")


def ReportFormat(key, period, tree, group):
    ANCScore = CombinedReport(key, period, tree)[0]
    PCVScore = CombinedReport(key, period, tree)[4]
    DelivScore = CombinedReport(key, period, tree)[7]
    Reporting = (
        (
            CombinedReport(key, period, tree)[2] + CombinedReport(key, period, tree)[5] +
            CombinedReport(key, period, tree)[8]) / 300) * 100
    TotalScore = ANCScore + PCVScore + DelivScore
    RankPosition = CombinedReport(key, period, tree)[9][0]
    # RankPositionOf = CombinedReport(key, period, tree)[9][1]
    # RankPositionTot = CombinedReport(key, period, tree)[9][2]
    RankPositionval = int(Ranking(key, period, tree))
    ANCScorePast = CombinedReport(key, period, tree)[1]
    PCVScorePast = CombinedReport(key, period, tree)[5]
    DelivScorePast = CombinedReport(key, period, tree)[8]
    TotalScorePast = ANCScorePast + PCVScorePast + DelivScorePast
    ret = {
        'month': current_month.capitalize(),
        'total_score': TotalScore,
        'anc_score': ANCScore,
        'delivery_score': DelivScore,
        'pcv_score': PCVScore,
        'reporting_rate': Reporting,
        'position': RankPosition,
        'comment': ''
    }
    if group == 1:
        if TotalScorePast < TotalScore:
            ret['comment'] = config["positive_comment"]
        else:
            ret["comment"] = config["negative_comment"]
    elif group == 2:
        if RankPositionval == 1 and Reporting > 0:
            ret['comment'] = config["positive_comment"]
        else:
            if Reporting > 0:
                ret['comment'] = config["position_comment"] % ret + config["improve_comment"]  # we can print the comment
            else:
                ret['comment'] = "Please make sure you report on 1stANC, PCV3 and Delivery"
    elif group == 3:
        if RankPositionval == 1 and Reporting > 0:
            ret['comment'] = config["positive_comment"]
        else:
            if Reporting > 0:
                ret['comment'] = config["position_comment"] % ret + config["improve_comment"]
            else:
                ret['comment'] = "Please make sure you report on 1stANC, PCV3 and Delivery"
    else:
        pass
    return ret


def Ranking(a, b, tree):
    RankDict = {}
    RankInitial = []
    RankInitial.append(ANC_reportRank(a, b, tree))
    c = RowToSub[a]
    total = 0.00
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, b, tree)
        total += 1
    del RankDict[a]
    RankList = RankDict.values()
    for x in RankList:
        RankInitial.append(x)
    RankPosition = Series(RankInitial)
    RankFinal = RankPosition.rank(method='min')[0]
    # print 'Ranked:',ord(RankFinal),'on a scale of:', total
    return RankFinal

conn = psycopg2.connect(
    "dbname=" + config["dbname"] + " host= " + config["dbhost"] + " port=" + config["dbport"] +
    " user=" + config["dbuser"] + " password=" + config["dbpasswd"])
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
# get the district dhis2ids for eligible districts
cur.execute("SELECT dhis2id, name FROM districts WHERE eligible = 't'")
res = cur.fetchall()

# for r in ["x75Yh65MaUa"]:
for r in res:
    value = read_csv_to_file(BASE_URL % r["dhis2id"])
    # value = read_csv_to_file2(BASE_URL % "")
    keys = value.keys()
    # computing the number of rows present in the data set, later to be used the ranking
    myrows = value.shape[0]
    # line0 = value.values[myrows][0]
    check = value.groupby(value['organisationunitname'])
    RowToSub = {}
    for i in range(myrows):
        RowToSub[value.values[i][0]] = value.values[i][1]

    # The above lists correspond to the column numbers of the data.
    # it is upon the basis that on export the data columns will not change, only the rows will change.
    # please note herein that the indices do not take account YET of the seldom used
    # "'Organisation unit'" and 'Organisation unit description

    Mydict1 = {}
    Mydict2 = {}
    Mydict = {}
    WholeTree = {}
    # herein ANC data is separated
    for i in range(myrows):
        Mylist = []
        for v in ancList:
            v += 3            # This additional line caters for the comment 3 above
            Mylist.append(value.values[i][v])
        Mydict[value.values[i][0]] = Mylist
    # Now we will consider the Deliveries in Unit data
    for i in range(myrows):
        Mylist = []
        for v in delivList:
            v += 3
            Mylist.append(value.values[i][v])
        Mydict1[value.values[i][0]] = Mylist
    # We'll now consider PVC data
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
    WholeTree['PVC'] = Mydict2

    # ReportFormat('imFgSY3OUvZ', 'apr', WholeTree, 1)
    for facilityid in RowToSub.keys():
        cur.execute("SELECT id, uuid FROM facilities WHERE dhis2id = '%s' FOR UPDATE NOWAIT" % facilityid)
        result = cur.fetchone()
        if result:
            facility_uuid = result["uuid"]
        else:  # fetch it from dhis2
            facilityurl = config["orgunits_url"] + "/" + facilityid + ".json"
            payload = {'fields': 'id,uuid,name'}
            try:
                resp = requests.get(facilityurl, params=payload, auth=(config["dhis2_user"], config["dhis2_passwd"]))
                f = json.loads(resp.text)
                if 'uuid' in f:
                    facility_uuid = f["uuid"]
            except:
                facility_uuid = ''
        # only generate report if we have a valid facility uuid
        if facility_uuid:
            report = ReportFormat(facilityid, current_month, WholeTree, 3)
            message = config["facility_report_template"] % report
            print message

conn.close()
