import requests


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


csv_download = get_url(BASE_URL % "x75Yh65MaUa")

# here each line has the facility record
for l in csv_download.split('\n')[1:2]:
    line = l.replace('"', '')
    data = line.split(',')
    print data[0]
    print data
