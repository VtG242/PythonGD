"""
General functions, variables used in other gd modules
"""

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

class GoodDataError(Exception):
    """Specific errors from Gooddata API or that class"""
