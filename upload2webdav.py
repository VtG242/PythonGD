import json
import traceback
import urllib2
import gdlogin
import zipfile
import os
import datetime

def print_info(archive_name):
    zf = zipfile.ZipFile(archive_name)
    for info in zf.infolist():
        print info.filename
        print '\tModified:\t', datetime.datetime(*info.date_time)
        print '\tCompressed:\t', info.compress_size, 'bytes'
        print '\tUncompressed:\t', info.file_size, 'bytes'
        print

# preparing zip
files = ["all.csv","allgrain.csv","upload_info.json"]

zf = zipfile.ZipFile("upload.zip", "w", zipfile.ZIP_DEFLATED)
for f in files:
    zf.write(f)

zf.close()

print_info('upload.zip')

#upload to webdav

# TT is used also for webdav
gl = gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com" ,"xxx")

f = open("upload.zip", 'r')
headers = gdlogin.http_headers_template.copy()
headers["X-GDC-AuthTT"] = gl.temporary_token
headers["Content-Length"] = os.path.getsize("upload.zip")

url = "https://secure-di.gooddata.com/uploads/new-directory3/upload.zip"
request = urllib2.Request(url, headers=headers, data=f.read())
request.get_method = lambda: 'PUT'

try:
    response = urllib2.urlopen(request)

except urllib2.HTTPError, emsg:
    print traceback.print_exc()
    #to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry
else:
    response_body = response.read()
    print response_body

gl.logout()

