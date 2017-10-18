import json
import traceback

from urllib2 import *
# using BaseHTTPRequestHandler.responses as dictionary for HTTP statuses
from BaseHTTPServer import BaseHTTPRequestHandler
from copy import deepcopy


class GoodDataError(Exception):
    """Specific errors from Gooddata API or that class"""
    pass


class GoodDataLogin():
    """Class which performs login to GoodData platform and manage obtaining of temporary token"""

    debug = False
    login_json_template = """
{
    "postUserLogin": {
    "login": "" ,
    "password": "" ,
    "remember": 1 ,
    "verify_level": 2
    }
}
    """
    http_headers_template = {
        "Content-Type": "application/json" ,
        "Accept": "application/json"
    }

    def __init__(self ,usr ,passwd ,gdhost="https://secure.gooddata.com"):
        """
        Initialization of GoodDataLogin object - mandatory arguments are Gooddata login and password.
        For another host than "secure" (white-label solution) use gdhost parameter.
        """
        self.gdhost = gdhost
        self.usr = usr
        self.passwd = passwd
        login_json = json.loads(GoodDataLogin.login_json_template)
        login_json["postUserLogin"]["login"] = usr
        login_json["postUserLogin"]["password"] = passwd

        try:

            # step 1 - /gdc/account/login
            headers = GoodDataLogin.http_headers_template.copy()
            url = self.gdhost + "/gdc/account/login"
            request = Request(url ,data=json.dumps(login_json) ,headers=headers)

            response = urlopen(request)
            response_body = response.read()

            account_login_response_json = json.loads(response_body)
            self.super_secured_token = account_login_response_json["userLogin"]["token"]
            self.login_profile = account_login_response_json["userLogin"]["state"]

            if GoodDataLogin.debug:
                print "Request: "
                print request.get_method() + " " + url
                print headers
                print json.dumps(login_json)
                print "Response: "
                print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
                print response_body
                print "Variables: "
                print "SST: " + self.super_secured_token
                print "Login: " + self.login_profile
                print "-" * 100

        except HTTPError ,emsg:
            if emsg.code == 401:
                raise GoodDataError(str(emsg) + " - please check your GoodData credentials and try it again.")
            elif emsg.code == 404:
                raise GoodDataError(str(
                    emsg) + " - there is problem with '" + url + "' please check that GoodData API endpoint is specified correctly.")
            else:
                raise Exception(emsg)
        except URLError ,emsg:
            raise GoodDataError(str(
                emsg) + " - there is problem with '" + self.gdhost + "' please check that GoodData host name is specified correctly.")

        # step 2 - /gdc/account/token - repeatable
        self.temporary_token = ""

    def __repr__(self):
        return "GoodDataLogin instance detail:\nhost = %s\nuser = %s profile = %s\nSST = %s" % (
        self.gdhost ,self.usr ,self.login_profile ,self.super_secured_token)

    def get_temporary_token(self):
        """
        Function returns temporary token which is required for any call to GoodData API.
        Temporary token is valid only short period of time (usually 10 minutes) so
        use the function also in case that 401 http code is returned when poll API resource for result.
        """

        if not self.super_secured_token:
            # SST is empty - probably logout() - reinitialize object
            GoodDataLogin.__init__(self ,self.usr ,self.passwd ,self.gdhost)

        try:

            headers = GoodDataLogin.http_headers_template.copy()
            headers["X-GDC-AuthSST"] = self.super_secured_token

            url = self.gdhost + "/gdc/account/token"
            request = Request(url ,headers=headers)

            response = urlopen(request)
            response_body = response.read()

            tt_json = json.loads(response_body)

            if GoodDataLogin.debug:
                print "Request: "
                print request.get_method() + " " + url
                print headers
                print json.dumps(tt_json)
                print "Response: "
                print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
                print response_body
                print "Variables: "
                print "TT: " + tt
                print "-" * 100

            return tt_json["userToken"]["token"]

        except HTTPError ,emsg:
            raise GoodDataError(str(emsg) + " - " + url + "' problem during obtaining of temporary token.")

    def logout(self):
        """Performs logout from GoodData - SST token is destroyed"""
        headers = {
            'Accept': 'application/json' ,
            'X-GDC-AuthSST': self.super_secured_token ,
            'X-GDC-AuthTT': self.get_temporary_token()
        }

        url = "https://secure.gooddata.com" + self.login_profile
        request = Request(url ,headers=headers)
        request.get_method = lambda: 'DELETE'

        response = urlopen(request)
        response_body = response.read()

        self.super_secured_token = ""

        if GoodDataLogin.debug:
            print "Request: "
            print request.get_method() + " " + url
            print str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])
            print "-" * 100


# test code
if __name__ == "__main__":

    try:

        GoodDataLogin.debug = False
        gl = GoodDataLogin("testuser@testdomain.com" ,"testpassword")
        tt = gl.get_temporary_token()

        # GoodDataLogin object text representation
        print gl
        # TT
        print "TT: " + tt
        print ""

        gl.logout()

    except GoodDataError ,emsg:
        print "GoodData error: " + str(emsg)

    except Exception ,emsg:
        print traceback.print_exc()
        print "General error: " + str(emsg)
