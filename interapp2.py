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
import simplejson
import re

user = config['dhis2_user']
passwd = config['dhis2_passwd']

# the month for which we're generating the reports
current_month = datetime.datetime.now().strftime('%B').lower()[:3]
pattern1 = re.compile(r'(HC III$)|(III$)|((.*\b) III (.*\b))')
pattern5 = re.compile(r'(HC II$)')
pattern6 = re.compile(r'(II$)')
pattern7 = re.compile(r'((.*) II (.*))')

def HCIII_reg(s):
    flag = pattern1.search(s)
    if flag:
        return True
    else:
        return False

def HCII_reg(s):
    if pattern5.search(s):
        return True
    elif pattern6.search(s) and not(HCIII_reg(s)):
        return True
    elif pattern7.search(s) and not(HCIII_reg(s)):
        return True
    else:
        return False

# Please note ancList below contains colum numbers of both anc 1st visit and anc 4th visit.
ancList = [5, 6, 9, 10, 13, 14, 17, 18, 21, 22, 25, 26]
delivList = [7, 11, 15, 19, 23, 27]
pvcList = [8, 12, 16, 20, 24, 28]

Months = {}
Mappings = {}

# start of Months Dictionary; this is important to maintain due to the need to report given months performace versus perfomrance from past
# period's report

# starting to change from past months report comparison to previous year's
# report, in the same period.
# hence the need to change Months and Mappings above.

# 'index' below refers to the location within the .csv download
# the srting 'pst' standing for past, represents the previous years
# data. |_0_|_1_|_2_|_3_|_4_|_5_| is an example of the array objext 
# data. index  0-2 represents the past year, whereas 3-5 represents 
# the present year.

Mappings['index'] = 'pst'

Months['index'] = [3, 4, 5]
Months['pst'] = [0, 1, 2]

# To handle Json in DB well
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


def generate_period_string():
    """Return 3 months period string for both the current report
    and similar report a year ago
    e.g 201404;201405;201406;201504;201505;201506 for July 2015"""
    t = datetime.datetime.now()
    year = t.year
    month = t.month
    if month <= 3:
        third = (month - 3) % 12 if (month - 3) < 0 else 12 if (month - 3) == 0 else (month - 3)
        second = (month - 2) % 12 if (month - 2) < 0 else 12 if (month - 2) == 0 else (month - 2)
        first = (month - 1) % 12 if (month - 1) < 0 else 12 if (month - 1) == 0 else (month - 1)

        str1 = "%s%02d;%s%02d;%s%02d" % (year - 1, third, year - 1, second, year - 1, first)
        str2 = "%s%02d;%s%02d;%s%02d" % (year, third, year, second, year, first)
    else:
        str1 = "%s%02d;%s%02d;%s%02d" % (year - 1, month - 3, year - 1, month - 2, year - 1, month - 1)
        str2 = "%s%02d;%s%02d;%s%02d" % (year, month - 3, year, month - 2, year, month - 1)
    return "%s;%s" % (str1, str2)


# BASE_URL will actuall point to the pivot table download we need
BASE_URL = config['base_url']
BASE_URL = BASE_URL + "dimension=pe:%s&" % generate_period_string()
BASE_URL = BASE_URL + "dimension=dx:yTtv6wuTWUN;eGSUL2aL0zW;OWJ3hkJ9VYA;iNVDqc0xKi0&dimension=ou:LEVEL-5;%s"
payload = {
    "filter": "u8EjsUj11nz:jTolsq2vJv8;GM7GlqjfGAW;luVzKLwlHJV",
    "tableLayout": "true",
    "columns": "pe;dx",
    "rows": "ou",
}


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


def send_facility_sms(params):  # params has the facility uuid and other params
    res = requests.get(config["smsurl"], params=params)
    return res.text


def read_csv_to_file2(url):
    pobj = pandas.read_csv("/tmp/2015-05-12_35_211431419721.csv")
    return pobj


def ord(n):
    return str(int(n)) + ("th" if 4 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def save_facility_record(conn, cur, fuuid, record):
    if "comment" in record:
        record.pop("comment")
    if "month" in record:
        record.pop("month")
    cur.execute("SELECT previous_values::text FROM facilities WHERE uuid = %s", [fuuid])
    res = cur.fetchone()
    if res:
        prev_vals = json.loads(res['previous_values'])
        prev_vals['%s' % datetime.datetime.now().strftime('%Y-%m')] = record
        cur.execute(
            "UPDATE facilities SET previous_values = %s WHERE uuid = %s",
            [psycopg2.extras.Json(prev_vals, dumps=simplejson.dumps), fuuid])
        conn.commit()


def CombinedReport(id, tree):
    """CombinedReport(id,tree)-- This takes an id and returns the ANC1st, ANC4th
    PVC, Deliveries score, as well as comparisons to previous year period
    """
    mytype = tree['ANC'][id]
    mynewtype = []
    mynewtype1st = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.nan_to_num(mynewtype)
    for i in Months['index']:
        x += remapped[i]
    A = x
    # adding in 6 lines below to differenciate ANC1stvisit ....
    # from ANC4th Visit
    alpha = 0
    for i in range(1, len(mytype), 2):
        mynewtype1st.append(mytype[i])
    auxlist = np.nan_to_num(mynewtype1st)
    for i in Months['index']:
        alpha += auxlist[i]
    anc_alpha = alpha
# start of comparison report bit of the function; that is ANC1st ...
# visit and ANC4th visit
    mytypeComp = tree['ANC'][id]
    mynewtypeComp = []
    mynewtypeComp1st = []
    xComp = 0
    xcomp1st = 0
    for i in range(0, len(mytypeComp), 2):
        mynewtypeComp.append(mytypeComp[i])
    remapped = np.nan_to_num(mynewtypeComp)
    for i in Months[Mappings['index']]:
        xComp += remapped[i]
    B = xComp
    if B > 0:
        ANC4thCompare = (float(A - B) / B) * 100
    elif A > 0:
        ANC4thCompare = 100
    else:
        ANC4thCompare = 0
    # comparison for ANC1st visit
    for i in range(1, len(mytypeComp), 2):
        mynewtypeComp1st.append(mytypeComp[i])
    auxremapped = np.nan_to_num(mynewtypeComp1st)
    for i in Months[Mappings['index']]:
        xcomp1st += auxremapped[i]
    anc_comp = xcomp1st
    if anc_comp > 0:
        ANC1stCompare = (float(anc_alpha - anc_comp) / anc_comp) * 100
    elif anc_alpha > 0:
        ANC1stCompare = 100
    else:
        ANC1stCompare = 0
# start of completeness report for ANC
    mytype = tree['ANC'][id]
    mynewtype = []
    truetest = []
    truetest1st = []
    mynewtypecomplete = []
    x = 0
    for i in range(0, len(mytype), 2):
        mynewtype.append(mytype[i])
    remapped = np.isnan(mynewtype)
    for i in Months['index']:
        truetest.append(remapped[i])
    remappednum = truetest.count(False)
    C = remappednum
    for i in range(1, len(mytype), 2):
        mynewtypecomplete.append(mytype[i])
    remappedcomplete = np.isnan(mynewtype)
    for i in Months['index']:
        truetest1st.append(remapped[i])
    remappednumcomplete = truetest1st.count(False)
    C_complete = remappednumcomplete
    mytype = tree['PVC'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months['index']:
        x += remapped[i]
    D = x
# start of comparison of PVC past Month's report
    mytype = tree['PVC'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings['index']]:
        x += remapped[i]
    E = x
    if E > 0:
        PVCCompare = (float(D - E) / E) * 100
    elif D > 0:
        PVCCompare = 100
    else:
        PVCCompare = 0
# start of comparison of Completeness of PVC past Month's report
    mytype = tree['PVC'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months['index']:
        truetest.append(remapped[i])
    x = truetest.count(False)
    F = x
    mytype = tree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months['index']:
        x += remapped[i]
    G = x
# start of comparison for the Past Months Deliveries report
    mytype = tree['Deliv'][id]
    x = 0
    remapped = np.nan_to_num(mytype)
    for i in Months[Mappings['index']]:
        x += remapped[i]
    H = x
    if H > 0:
        DelivCompare = (float(G - H) / H) * 100
    elif G > 0:
        DelivCompare = 100
    else:
        DelivCompare = 0
# start of comparison of Completeness of Deliveries for Month's report
    mytype = tree['Deliv'][id]
    truetest = []
    x = 0.0000
    remapped = np.isnan(mytype)
    remapped = list(remapped)
    for i in Months['index']:
        truetest.append(remapped[i])
    x = truetest.count(False)
    I = x
    # Now we'll come up with a general completeness calculation:
    # This takes a count of the True values amongst all variables, divided by 12:
    # that is 4 variables over 3 months;
    truesum = I + F + C_complete + C
    truesum_complete = float(truesum / 12) * 100
    RankDict = {}
    RankInitial = []
    RankInitial.append(ANC_reportRank(id, tree))
    # The rank of id is first added to the RankInitial List, later to be de-duplicated..
    # since the loop in row 204 runs for all id's including the id we just submitted.
    # considering the rank method returns ranks by position, id's rank is thus read off...
    # from position 0.
    c = RowToSub[id]
    total = 0
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, tree)
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
    return A, anc_alpha, D, G, truesum_complete, L, DelivCompare, PVCCompare, ANC1stCompare, ANC4thCompare


xrand = np.random.randint(1, 5, size=1)
randomdict = {1: 'ANC', 2: 'ANC', 3: 'Deliv', 4: 'PVC'}


def ANC_reportRank(id, tree):
    """ANC_reportRank(id, tree)-- This takes an id and interval interms of shorthand "
    months of the year --E.g ANC_reportRank('cK5zkZIUFsN', WholeTree)"""
    # Given the change proposed by Zac, i.e randomization of comparison variables, need arose
    # to add a few lines to generate a random number.
    # As you may notice from above, key 1, and 2 of the dict refer to the same value...
    # this was mainly due to an earlier merge of the two, fixing this in the entire "WholeTree"
    # dict would be tedious hence the workaround below...
    mytype = tree[randomdict[xrand[0]]][id]
    mynewtype = []
    Q = 0
    P = 0
    xp = 0
    if xrand[0] == 1:
        xrandaplha = 0
        for i in range(xrandaplha, len(mytype), 2):
            mynewtype.append(mytype[i])
        remapped = np.nan_to_num(mynewtype)
        for i in Months['index']:
            Q += remapped[i]
        for i in Months[Mappings['index']]:
            xp += remapped[i]
        if xp > 0:
            Compare = (float(Q - xp) / xp) * 100
        elif Q > 0:
            Compare = 100
        else:
            Compare = 0
        Q = Compare
    elif xrand[0] == 2:
        xrandaplha = 1
        for i in range(xrandaplha, len(mytype), 2):
            mynewtype.append(mytype[i])
        remapped = np.nan_to_num(mynewtype)
        for i in Months['index']:
            Q += remapped[i]
        for i in Months[Mappings['index']]:
            xp += remapped[i]
        if xp > 0:
            Compare = (float(Q - xp) / xp) * 100
        elif Q > 0:
            Compare = 100
        else:
            Compare = 0
        Q = Compare
    else:
        remapped = np.nan_to_num(mytype)
        for i in Months['index']:
            P += remapped[i]
        for i in Months[Mappings['index']]:
            xp += remapped[i]
        if xp > 0:
            Compare = (float(P - xp) / xp) * 100
        elif P > 0:
            Compare = 100
        else:
            Compare = 0
        P = Compare
    return Q + P

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


def ReportFormat(key, tree, group):
    # A, anc_alpha, D, G, truesum_complete, L, DelivCompare, PVCCompare, ANC1stCompare, ANC4thCompare
    ANCScore = CombinedReport(key, tree)[1]
    ANC4thscore = CombinedReport(key, tree)[0]
    PCVScore = CombinedReport(key, tree)[2]
    DelivScore = CombinedReport(key, tree)[3]
    TotalScore = ANCScore + PCVScore + DelivScore
    RankPosition = CombinedReport(key, tree)[5][0]
    RankPositionval = int(Ranking(key, tree))
    ANCScoreCompare = CombinedReport(key, tree)[8]
    ANCScore4thCompare = CombinedReport(key, tree)[9]
    PCVScoreCompare = CombinedReport(key, tree)[7]
    DelivScoreCompare = CombinedReport(key, tree)[6]
    completenesswatch = CombinedReport(key, tree)[4]
    ret = {
        'month': current_month.capitalize(),
        'total_score': float(TotalScore.__format__('.2f')),
        'anc_score': float(ANCScore.__format__('.2f')),
        'delivery_score': float(DelivScore.__format__('.2f')),
        'pcv_score': float(PCVScore.__format__('.2f')),
        'reporting_rate': float(completenesswatch.__format__('.2f')),
        'position': RankPosition,
        'comment': ''
    }
    '''if group == 1:
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
                ret['comment'] = config["no_report_comment"]
    elif group == 3:
        if RankPositionval == 1 and Reporting > 0:
            ret['comment'] = config["positive_comment"]
        else:
            if Reporting > 0:
                ret['comment'] = config["position_comment"] % ret + config["improve_comment"]
            else:
                ret['comment'] = config["no_report_comment"]
    else:
        pass'''
    return ret


def Ranking(a, tree):
    RankDict = {}
    RankInitial = []
    RankInitial.append(ANC_reportRank(a, tree))
    c = RowToSub[a]
    total = 0.00
    for i in check:
        result = check.get_group(c)
    k = result.shape[0]
    for j in range(k):
        p = result.values[j][0]
        RankDict[p] = ANC_reportRank(p, tree)
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
    for i in range(myrows):
        if HCII_reg(value.loc[i,'organisationunitname']):
            value.loc[i,'organisationunitname'] = value.loc[i,'organisationunitname'].replace(value.loc[i,'organisationunitname'], "HC II")
        elif HCIII_reg(value['organisationunitname'][i]):
            value.loc[i,'organisationunitname'] = value.loc[i,'organisationunitname'].replace(value.loc[i,'organisationunitname'], "HC III")
        else:
            pass
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
            v -= 1
            Mylist.append(value.values[i][v])
        Mydict[value.values[i][0]] = Mylist
    # Now we will consider the Deliveries in Unit data
    for i in range(myrows):
        Mylist = []
        for v in delivList:
            v -= 1
            Mylist.append(value.values[i][v])
        Mydict1[value.values[i][0]] = Mylist
    # We'll now consider PVC data
    for i in range(myrows):
        Mylist = []
        for v in pvcList:
            v -= 1
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
            report = ReportFormat(facilityid, WholeTree, 2)
            message = config["facility_report_template"] % report
            params = {
                'fuuid': facility_uuid,
                'text': message,
                'username': config['smsuser'],
                'password': config['smspasswd'],
                'district': r["name"],
            }
            # send_facility_sms(params)
            save_facility_record(conn, cur, facility_uuid, report)

conn.close()
