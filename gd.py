"""
General functions, variables used in other gd modules
"""
import json
import sys

# using BaseHTTPRequestHandler.responses as dictionary for HTTP statuses
from BaseHTTPServer import BaseHTTPRequestHandler

http_headers_template = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def request_info(request):
    if request.data:
        try:
            # TODO - quick and dirty hack - find a better way how to identify that the string has binary data
            unicode(request.data[0:100], "utf-8")
        except Exception as e:
            data = "\nData: binary"
        else:
            data = "\nData: " + request.data
    else:
        data = ""
    return "REQUEST: {} {}\nHeaders: {}{}".format(request.get_method(), request.get_full_url(), json.dumps(request.headers), data)


def response_info(api_response):
    return "RESPONSE: {}\n{}\n{}\n".format(api_response["code"], api_response["info"], api_response["body"])


def check_response(response):
    return {"body": response.read(), "info": response.info(),
            "code": str(response.code) + " " + str(BaseHTTPRequestHandler.responses[response.code])}


def show_error(err_msg_json):
    err_parrams = err_msg_json["error"]["parameters"]
    err_message = err_msg_json["error"]["message"]
    return err_message % (tuple(err_parrams))


class GoodDataError(Exception):
    """General errors from GoodData library"""

    def __init__(self, msg, original_exception=None):
        if original_exception is None:
            self.msg = msg
        else:
            self.msg = "{}: {}".format(msg,original_exception)
        super(GoodDataError, self).__init__(self.msg)
        self.original_exception = original_exception

    def __str__(self):
        return "{}".format(self.msg)


class GoodDataAPIError(Exception):
    """Specific errors from GoodData API or related"""

    def __init__(self, url, original_exception, msg=None):
        if msg is None:  # Set some default useful error message
            msg = "An error occurred during API operation"
        super(GoodDataAPIError, self).__init__("{}: {}".format(msg,original_exception))
        self.url = url
        self.msg = msg
        self.original_exception = original_exception

        if hasattr(original_exception, "read"):
            gd_err_msg = original_exception.read()
            try:
                # in case that response isn't json or hasn't message key saving as is in detail
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
        return "{}: {}\nUrl: {}\nMessage: {}".format(self.msg, self.original_exception, self.url, self.detail)
