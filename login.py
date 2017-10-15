import json
import traceback
import BaseHTTPServer
import urllib2
from urllib2 import Request, urlopen

try:

    # login data
    values = """
    {
        "postUserLogin":{
            "login":"test@foo.com",
            "password":"testpass",
            "remember":0,
            "verify_level":2
        }
    }"""
    headers = {
        'Content-Type': 'application/json' ,
        'Accept': 'application/json'
    }
    # API endpoint
    url = "https://secure.gooddata.com/gdc/account/login"
    request = Request(url ,data=values ,headers=headers)

    response = urlopen(request)
    response_body = response.read()

    print request.get_method() + " " + url
    print str(response.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[response.code])
    print response_body

    login_json = json.loads(response_body)
    super_secured_token = login_json["userLogin"]["token"]
    login_profile = login_json["userLogin"]["state"]

    print "SST: " + super_secured_token
    print "login: " + login_profile
    print "-" * 100  # TT
    headers = {
        'Accept': 'application/json' ,
        'Content-Type': 'application/json' ,
        'X-GDC-AuthSST': super_secured_token
    }

    url = "https://secure.gooddata.com/gdc/account/token"
    request = Request(url ,headers=headers)

    response = urlopen(request)
    response_body = response.read()

    print request.get_method() + " " + url
    print str(response.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[response.code])
    print response_body

    tt_json = json.loads(response_body)
    tt = tt_json["userToken"]["token"]

    print "TT: " + tt
    print "-" * 100

    # main API action
    headers = {
        'Accept': 'application/json' ,
        'Content-Type': 'application/json' ,
        'X-GDC-AuthTT': tt
    }
    url = "https://secure.gooddata.com/gdc/datawarehouse"
    request = Request(url ,headers=headers)

    response = urlopen(request)
    response_body = response.read()

    print request.get_method() + " " + url
    print str(response.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[response.code])
    print response_body

    warehouse_json = json.loads(response_body)
    print warehouse_json
    print "-" * 100

    # Logout
    headers = {
        'Accept': 'application/json' ,
        'X-GDC-AuthSST': super_secured_token ,
        'X-GDC-AuthTT': tt
    }

    url = "https://secure.gooddata.com" + login_profile
    request = Request(url ,headers=headers)
    request.get_method = lambda: 'DELETE'

    response = urlopen(request)
    response_body = response.read()

    print request.get_method() + " " + url
    print str(response.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[response.code])
    print response_body

except urllib2.HTTPError, e:
    print "Login to GD failed: " + str(e)
except Exception:
    print Exception
    print traceback.print_exc()