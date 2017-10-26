"""
General functions, variables used in other gd modules
"""
import json

# using BaseHTTPRequestHandler.responses as dictionary for HTTP statuses
from BaseHTTPServer import BaseHTTPRequestHandler

http_headers_template = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def debug_info(request, api_response, url, headers, payload=""):
    print "-" * 100
    print "* REQUEST:"
    print request.get_method() + " " + url
    print "Headers: " + headers
    if payload: print "Payload: " + payload
    print "\n* RESPONSE:"
    print api_response["info"]
    print api_response["code"] + "\n"
    print api_response["body"]
    print "-" * 100


def check_response(response):
    return {"body": response.read(), "info": response.info(),
            "code": str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])}


def show_error(err_msg_json):
    err_parrams = err_msg_json["error"]["parameters"]
    err_message = err_msg_json["error"]["message"]
    return err_message % (tuple(err_parrams))


class GoodDataError(Exception):
    """General errors from GoodData library"""


class GoodDataAPIError(Exception):
    """Specific errors from GoodData API or related"""

    def __init__(self, url, original_exception, msg=None):
        if msg is None:  # Set some default useful error message
            msg = "An error occurred during API operation"
        super(GoodDataAPIError, self).__init__(msg + (": %s" % original_exception))
        self.url = url
        self.msg = msg
        self.original_exception = original_exception

        if hasattr(original_exception, 'read'):
            gd_err_msg = original_exception.read()
            try:
                # in case that response isn't json or hasn't message key saving as in in detail
                gd_err_msg = json.loads(gd_err_msg)

                if gd_err_msg.has_key("message"):
                    error_msg_dict = gd_err_msg
                else:
                    for slovnik in gd_err_msg:
                        if gd_err_msg[slovnik].has_key("message"):
                            error_msg_dict = gd_err_msg[slovnik]
                            break
                self.detail = error_msg_dict["message"] % (tuple(error_msg_dict["parameters"]))

            except Exception as e:
                # response isn't most probably json or other error during parsing
                self.detail = gd_err_msg
        elif hasattr(original_exception, "reason"):
            self.detail = original_exception.reason
        else:
            self.detail = "[empty]"


    def __str__(self):
        return "%s: %s\nUrl: %s\nMessage: %s" % (self.msg, self.original_exception, self.url, self.detail)
