import json
import traceback
import urllib2
import os
import zipfile
import gd
import gdlogin
import datetime


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
            # if directory infrastructure doesn't exist let's create it
            create_dir_if_not_exists(os.path.join(self.wd))
            create_dir_if_not_exists(os.path.join(self.wd, self.project))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "csv"))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "manifests"))

            # write to file in dir manifests in etl working directory
            for i in range(len(self.datasets)):
                with open(os.path.join(self.wd, self.project, "manifests", self.datasets[i] + "_upload_info.json"),
                          "w") as f:
                    f.write(json.dumps(self.manifests[i], sort_keys=True, indent=2, separators=(',', ': ')))
            # write csv file with template header to csv dir
            for i in range(len(self.datasets)):
                with open(os.path.join(self.wd, self.project, "csv", self.datasets[i] + "_header.csv"), "w") as f:
                    pom = ""
                    for attr in self.csv_header_templates[i]:
                        pom += '"%s",' % attr
                    f.write(pom[0:-1])

            # create final upload_info.json
            if len(datasets) > 1:  # we create SLI BATCH manifest
                upload_info_json = {"dataSetSLIManifestList": []}
                for manifest in self.manifests:
                    upload_info_json["dataSetSLIManifestList"].append(manifest)
            else:  # single manifest
                upload_info_json = dict(self.manifests[0])

            # write final upload_info.json to manifests directory
            with open(os.path.join(self.wd, self.project, "manifests", "upload_info.json"), "w") as f:
                f.write(json.dumps(upload_info_json, sort_keys=True, indent=2, separators=(',', ': ')))

        except OSError as e:
            raise gd.GoodDataError(e, "Problem with etl working directory")
        except IOError as e:
            raise gd.GoodDataError(e, "Problem during file operation")

    def perform_upload(self):
        """
        This method performs upload to user staging directory (WebDav)
        As this method can be also called directly(without calling prepare_upload() after creating GoodDataETL instance
        we have to check that all necessary files are in place.
        """
        if self.datasets and self.manifests:
            # this means that prepare_upload() has been ran so we most probably should have all files in place
            pass
        else:
            print "TODO: We need to read files from upload_info.json"
            # etl/pull2

        # creating upload.zip
        try:
            files = []
            # adding csv files with data for upload
            for dataset in self.datasets:
                files.append(os.path.join(self.wd, self.project, "csv", dataset + ".csv"))
            # adding current manifest
            files.append(os.path.join(self.wd, self.project, "manifests", "upload_info.json"))

            zf = zipfile.ZipFile(os.path.join(self.wd, self.project, "upload.zip"), "w", zipfile.ZIP_DEFLATED)
            for f in files:
                zf.write(f, os.path.basename(f))
            zf.close()

            zf = zipfile.ZipFile(os.path.join(self.wd, self.project, "upload.zip"))
            with open(os.path.join(self.wd, self.project, "upload.txt"), "w") as f:
                for info in zf.infolist():
                    f.write("%s\n" % info.filename)
                    f.write("\tModified:\t%s\n" % datetime.datetime(*info.date_time))
                    f.write("\tCompressed:\t%d bytes\n" % info.compress_size)
                    f.write("\tUncompressed:\t%d bytes\n" % info.file_size)
        except Exception as e:
            raise gd.GoodDataError(e,
                                   "Problem during creating upload.zip - check that all source files for upload are in csv directory")

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


def create_dir_if_not_exists(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)

# test code
if __name__ == "__main__":

    GoodDataETL.debug = False

    try:
        etl = GoodDataETL(gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com", "vtgdevel"),
                          "gmlgncezgyatnnr0d1mc6tss82olgf0s")
        etl.prepare_upload("./etlwd", ["allgrain", "all"])
        etl.perform_upload()
        etl.glo.logout()
    except gd.GoodDataError as e:
        print e
    except gd.GoodDataAPIError as e:
        print e
    except Exception, emsg:
        print traceback.print_exc()
        print "General error: " + str(emsg)