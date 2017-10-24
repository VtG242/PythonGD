import json
import traceback
import urllib2
import gd


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

    def __init__(self, usr, passwd, gdhost="https://secure.gooddata.com"):
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

        # step 1 - /gdc/account/login
        headers = gd.http_headers_template.copy()
        url = self.gdhost + "/gdc/account/login"
        request = urllib2.Request(url, data=json.dumps(login_json), headers=headers)

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, emsg:
            if emsg.code == 401:
                raise gd.GoodDataError(str(emsg) + " - please check your GoodData credentials and try it again.")
            elif emsg.code == 404:
                raise gd.GoodDataError(str(
                    emsg) + " - there is problem with '" + url + "' please check that GoodData API endpoint is specified correctly.")
            else:
                raise Exception(emsg)
        except urllib2.URLError, emsg:
            raise gd.GoodDataError(str(
                emsg) + " - there is problem with '" + self.gdhost + "' please check that GoodData host name is specified correctly.")
        else:
            api_response = gd.check_response(response)

            account_login_response_json = json.loads(api_response["body"])
            self.login_profile = account_login_response_json["userLogin"]["state"]
            self.super_secured_token = account_login_response_json["userLogin"]["token"]
            # temporary token is returned in API response as X-GDC-AuthTT header
            self.temporary_token = api_response["info"]["X-GDC-AuthTT"]

            if GoodDataLogin.debug:
                gd.debug_info(request, api_response, url, json.dumps(headers), json.dumps(login_json))

            response.close()

    def __repr__(self):
        return "GoodDataLogin instance detail:\nhost = %s\nuser = %s profile = %s\nSST = %s\nTT = %s" % (
            self.gdhost, self.usr, self.login_profile, self.super_secured_token, self.temporary_token)

    def generate_temporary_token(self):
        """
        Function returns temporary token which is required for any call to GoodData API.
        Temporary token is valid only short period of time (usually 10 minutes) so
        use the function also in case that 401 http code is returned when poll API resource for result.
        """
        headers = gd.http_headers_template.copy()
        headers["X-GDC-AuthSST"] = self.super_secured_token
        url = self.gdhost + "/gdc/account/token"
        request = urllib2.Request(url, headers=headers)

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, emsg:
            if emsg.code == 401:
                """ we shouldn't receive unauthorized here - this is handled in __init__ - most probably it means that SST is no longer valid or logout() had been called
                - reinitialize of instance needed """
                if GoodDataLogin.debug: print "* 401 caught during TT call - calling for valid SST."
                GoodDataLogin.__init__(self, self.usr, self.passwd, self.gdhost)
            else:
                raise gd.GoodDataError(str(emsg) + " - " + url + "' problem during obtaining of temporary token.")
        else:
            api_response = gd.check_response(response)
            if GoodDataLogin.debug:
                gd.debug_info(request, api_response, url, json.dumps(headers))
            response.close()

            self.temporary_token = json.loads(api_response["body"])["userToken"]["token"]

        # token is set correctly here because even in case of 401 error is set in __init__()
        return self.temporary_token

    def logout(self):
        """Performs logout from GoodData - SST token is destroyed"""
        headers = {
            'Accept': 'application/json',
            'X-GDC-AuthSST': self.super_secured_token,
            'X-GDC-AuthTT': self.generate_temporary_token()
        }

        url = "https://secure.gooddata.com" + self.login_profile
        request = urllib2.Request(url, headers=headers)
        request.get_method = lambda: 'DELETE'

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, emsg:
            raise gd.GoodDataError(str(emsg) + " - " + url + "' problem during logout.")
        else:
            api_response = gd.check_response(response)
            self.super_secured_token = ""
            if GoodDataLogin.debug:
                gd.debug_info(request, api_response, url, json.dumps(headers))
            response.close()

    def save_to_file(self, fname):
        import pickle
        f = open(fname, "w")
        pickle.dump(self, f)
        f.close()


# test code
if __name__ == "__main__":

    try:

        GoodDataLogin.debug = True
        gl = GoodDataLogin("vladimir.volcko+sso@gooddata.com", "xxx")
        # GoodDataLogin object text representation
        print gl
        # for main API call we need call get_temporary_token() function which returns TT token (X-GDC-AuthTT)
        gl.save_to_file("gd.auth")
        # gl.logout()

    except gd.GoodDataError, emsg:
        print "GoodData error: " + str(emsg)

    except Exception, emsg:
        print traceback.print_exc()
        print "General error: " + str(emsg)
