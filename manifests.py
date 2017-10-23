import json
import traceback
import urllib2
import gdlogin
import time

# using BaseHTTPRequestHandler.responses as dictionary for HTTP statuses
from BaseHTTPServer import BaseHTTPRequestHandler

project = "gmlgncezgyatnnr0d1mc6tss82olgf0s"
datasets = ["all"]
manifests = []

gl=gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com","xxx")

#manifest download
headers = gdlogin.http_headers_template.copy()
headers["X-GDC-AuthTT"] = gl.temporary_token

for dataset in datasets:
    url = gl.gdhost + "/gdc/md/" + project + "/ldm/singleloadinterface/dataset." + dataset + "/manifest"
    request = urllib2.Request(url, headers=headers)

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
        api_response = gdlogin.check_GD_response(response)
        gdlogin.debug_info(request,api_response,url,json.dumps(headers))

        manifest_json = json.loads(api_response["body"])
        #print manifest_json["dataSetSLIManifest"]["parts"]

        p=0
        for m_column in manifest_json["dataSetSLIManifest"]["parts"]:
            if str(m_column["columnName"]).find(".dt_") > -1:
                human_readable_name = str(m_column["columnName"]).split("_")[-2] + "(" + m_column["constraints"]["date"] + ")"
            else:
                human_readable_name = str(m_column["columnName"]).split("_")[-1]

            print human_readable_name
            manifest_json["dataSetSLIManifest"]["parts"][p]["columnName"] = human_readable_name
            p += 1

        manifest_json["dataSetSLIManifest"]["file"] = dataset + ".csv"
        print json.dumps(manifest_json)

        #write to file
        with open(dataset + "_upload_info.json", "w") as f:
            f.write(json.dumps(manifest_json, sort_keys=True, indent=2, separators=(',', ': ')))

        #storing each manifest in list
        manifests.append(manifest_json)

#create upload.zip
if len(datasets) > 1: #we create SLI BATCH manifest
    upload_info_json = {"dataSetSLIManifestList":[]}
    print type(upload_info_json)
    for manifest in manifests:
        print manifest
        upload_info_json["dataSetSLIManifestList"].append(manifest)
else:
    upload_info_json = dict(manifests[0])

print upload_info_json
with open("upload_info.json", "w") as f:
    f.write(json.dumps(upload_info_json, sort_keys=True, indent=2, separators=(',', ': ')))
#upload files to webdav

gl.logout()