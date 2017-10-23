import json
import traceback
import urllib2
import gdlogin
import time

# using BaseHTTPRequestHandler.responses as dictionary for HTTP statuses
from BaseHTTPServer import BaseHTTPRequestHandler

gl=gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com","xxx")

# nastreleni etl tasku
headers = gdlogin.http_headers_template.copy()
headers["X-GDC-AuthTT"] = gl.temporary_token
url = gl.gdhost + "/gdc/md/gmlgncezgyatnnr0d1mc6tss82olgf0s/etl/pull2"
payload = {"pullIntegration":"new-directory"}
request = urllib2.Request(url, data=json.dumps(payload), headers=headers)

try:
    response = urllib2.urlopen(request)

except urllib2.HTTPError, emsg:
    # for 40x errors we don't retry
    if emsg.code==404:
        print str(emsg) + " - GoodData project doesn't exist."
    elif emsg.code==403:
        print str(emsg) + " - admin or editor role is required."
    else:
        print traceback.print_exc()
        #to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry
else:

    # 201 created
    response_body = response.read()
    etlpull2_json = json.loads(response_body)
    poll_url = etlpull2_json["pull2Task"]["links"]["poll"]

    print "* Request: "
    print request.get_method() + " " + url
    print "Headers: " + str(headers)
    print "* Response: "
    print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
    print response.info()
    print response_body
    print "-" * 100

    response.close()

"""
# pollujeme pro vysledek 1
headers = gdlogin.http_headers_template.copy()
headers["X-GDC-AuthTT"] = gl.temporary_token
url = gl.gdhost + poll_url
request = urllib2.Request(url, headers=headers)

try:
    response = urllib2.urlopen(request)

except urllib2.HTTPError, emsg:
    if emsg.code==401:
        # it seems that a new temporary token should be generated and we need to repeat poll request
        headers["X-GDC-AuthTT"] = gl.temporary_token
        request = urllib2.Request(url, headers=headers)
        # I don't expect (naively) another exception here so no another try: block
        response = urllib2.urlopen(request)
    else:
        print traceback.print_exc()
        #to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry

# poll response
response_body = response.read()
etl_task_state = json.loads(response_body)["wTaskStatus"]["status"]

print "* Request: "
print request.get_method() + " " + url
print "Headers: " + str(headers)
print "* Response: "
print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
print response.info()
print response_body
print "-" * 100
print etl_task_state

if etl_task_state == "RUNNING":
    pass
elif etl_task_state == "OK":
    pass
elif etl_task_state == "CANCELED":
    pass
else:
    # ERROR
    pass

response.close()
gl.logout()
"""

# pollujeme pro vysledek 2 - full result
headers = gdlogin.http_headers_template.copy()
headers["X-GDC-AuthTT"] = gl.temporary_token
url = gl.gdhost + poll_url
etl_task_state = "RUNNING"

while etl_task_state == "RUNNING" :
    # current request
    request = urllib2.Request(url, headers=headers)

    try:
        response = urllib2.urlopen(request)

    except urllib2.HTTPError, emsg:
        if emsg.code==401:
            # it seems that a new temporary token should be generated and we need to repeat poll request
            headers["X-GDC-AuthTT"] = gl.generate_temporary_token()
            continue
        else:
            print traceback.print_exc()
            break
            #to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry

    # poll response
    response_body = response.read()
    etl_task_state = json.loads(response_body)["wTaskStatus"]["status"]
    """
    print "* Request: "
    print request.get_method() + " " + url
    print "Headers: " + str(headers)
    print "* Response: "
    print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
    print response.info()
    print "-" * 100
    """
    print response_body
    print etl_task_state
    time.sleep(3)

if etl_task_state == "ERROR":
    err_parrams = json.loads(response_body)["wTaskStatus"]["messages"][0]["error"]["parameters"]
    err_message = json.loads(response_body)["wTaskStatus"]["messages"][0]["error"]["message"]
    print err_parrams, err_message
    print err_message % (tuple(err_parrams))
else:
    print response_body

response.close()
gl.logout()
