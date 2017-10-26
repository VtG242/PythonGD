import json
import traceback
import urllib2
import os
import gd
import gdlogin


class GoodDataETL():
    # class debug variable
    debug = False

    def __init__(self, globject, project, debug=False):
        self.glo = globject
        self.project = project
        # instance debug variable
        self.debug = debug if True else False
        self.wd = "./etlwd"
        self.datasets = []
        self.manifests = []
        self.csv_header_templates = []

    def prepare_upload(self, working_directory, datasets):
        """ This function downloads manifest/s for given list of datasets from GoodData API
        1) modify names for csv columns to more human readable names
        2) save manifests in etl working directory in project/manifest
        3) save template csv with headers in etl working directory in project/csv
        4) creates final upload_info.json - in case of more datasets it creates batch mode manifest
        """
        self.wd = working_directory
        self.datasets = datasets
        self.datasets.sort()

        # according to datasets we will download and consequently modify manifests for specified dataset
        headers = gd.http_headers_template.copy()
        headers["X-GDC-AuthTT"] = etl.glo.generate_temporary_token()

        for dataset in self.datasets:
            url = etl.glo.gdhost + "/gdc/md/" + self.project + "/ldm/singleloadinterface/dataset." + dataset + "/manifest"
            request = urllib2.Request(url, headers=headers)

            try:
                response = urllib2.urlopen(request)
            except urllib2.HTTPError as e:
                # for 40x errors we don't retry
                if e.code == 403 or e.code == 404:
                    raise gd.GoodDataAPIError(url, e, msg="Problem during retrieving a manifest")
                else:
                    raise Exception(e)
                    # to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry
            else:

                api_response = gd.check_response(response)

                if GoodDataETL.debug | self.debug:
                    gd.debug_info(request, api_response, url, json.dumps(headers))

                manifest_json = json.loads(api_response["body"])

                p = 0
                csv_header_template = []
                for m_column in manifest_json["dataSetSLIManifest"]["parts"]:
                    # for date we use in name date format name(yyyy-MM-dd)
                    if str(m_column["columnName"]).find(".dt_") > -1:
                        human_readable_name = str(m_column["columnName"]).split("_")[-2] + "(" + \
                                              m_column["constraints"]["date"] + ")"
                    else:  # for other attributtes we will pick name after last _
                        human_readable_name = str(m_column["columnName"]).split("_")[-1]

                    # instead of originally generated names we will use human readable name of attribute
                    manifest_json["dataSetSLIManifest"]["parts"][p]["columnName"] = human_readable_name
                    csv_header_template.append(human_readable_name)
                    p += 1

                # also name of csv file is changed within manifest
                manifest_json["dataSetSLIManifest"]["file"] = dataset + ".csv"

                # save each csv_header_template to list for later usage
                csv_header_template.sort()
                self.csv_header_templates.append(csv_header_template)
                # storing each manifest in list for later usage
                self.manifests.append(manifest_json)

        # writing manifests and csv templates files to etl working directory
        try:
            # if directory infrastructure doesn't exist we create missing dirs
            if not os.path.isdir(self.wd + "/" + self.project):
                os.makedirs(self.wd + "/" + self.project)
            if not os.path.isdir(self.wd + "/" + self.project + "/csv"):
                os.makedirs(self.wd + "/" + self.project + "/csv")
            if not os.path.isdir(self.wd + "/" + self.project + "/manifests"):
                os.makedirs(self.wd + "/" + self.project + "/manifests")

            self.save_manifests_and_templates()

            # create final upload_info.json
            if len(datasets) > 1:  # we create SLI BATCH manifest
                upload_info_json = {"dataSetSLIManifestList": []}
                for manifest in self.manifests:
                    upload_info_json["dataSetSLIManifestList"].append(manifest)
            else:  # single manifest
                upload_info_json = dict(self.manifests[0])

            # write final upload_info.json to manifests directory
            with open(self.wd + "/" + self.project + "/manifests/upload_info.json", "w") as f:
                f.write(json.dumps(upload_info_json, sort_keys=True, indent=2, separators=(',', ': ')))

        except OSError as e:
            raise gd.GoodDataError(e, "Problem with etl working directory")

    def perform_upload(self):
        pass
        # upload to webdav
        # etl/pull2

    def save_manifests_and_templates(self):
        # write to file in dir manifests in etl working directory
        for i in range(len(self.datasets)):
            with open(self.wd + "/" + self.project + "/manifests/" + self.datasets[i] + "_upload_info.json", "w") as f:
                f.write(json.dumps(self.manifests[i], sort_keys=True, indent=2, separators=(',', ': ')))
        # write csv file with template header to csv dir
        for i in range(len(self.datasets)):
            with open(self.wd + "/" + self.project + "/csv/" + self.datasets[i] + "_header.csv", "w") as f:
                pom = ""
                for attr in self.csv_header_templates[i]:
                    pom += '"%s",' % attr
                f.write(pom[0:-1])


# test code
if __name__ == "__main__":

    GoodDataETL.debug = False

    try:
        etl = GoodDataETL(gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com", "xxx"),
                          "gmlgncezgyatnnr0d1mc6tss82olgf0s")
        etl.prepare_upload("./etlwd", ["allgrain", "all"])

        etl.glo.logout()
    except gd.GoodDataError as e:
        print e
    except gd.GoodDataAPIError as e:
        print e
    except Exception, emsg:
        print traceback.print_exc()
        print "General error: " + str(emsg)
