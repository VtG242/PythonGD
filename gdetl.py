# -*- coding: utf-8 -*-
import json
import traceback
import urllib2
import os
import zipfile
import gd
import gdlogin
import datetime
import csv
import logging
import uuid
import time

logger = logging.getLogger(__name__)


class GoodDataETL():
    def __init__(self, globject, project, working_directory):
        self.glo = globject
        self.project = project
        self.wd = working_directory
        self.manifests = []
        self.csv_header_templates = []
        self.remote_etl_dir = ""
        self.etl_task_result = "N/A"
        # {[dataset][mode]}
        self.datasets = {}
        # creating of a working_directory and set self.manifests in case that upload_info.json exists in manifests dir
        try:
            create_dir_if_not_exists(self.wd)
            create_dir_if_not_exists(os.path.join(self.wd, self.project))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "csv"))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "manifests"))

            # in case that upload_info.json exists we will fill self.datasets using this file
            if os.path.isfile(os.path.join(self.wd, self.project, "manifests", "upload_info.json")):
                with open(os.path.join(self.wd, self.project, "manifests", "upload_info.json"), "r") as f:
                    upload_info_json = json.loads(f.read())

                if upload_info_json.has_key("dataSetSLIManifestList"):
                    # slibatch mode
                    for manifest in upload_info_json["dataSetSLIManifestList"]:
                        self.datasets[manifest["dataSetSLIManifest"]["dataSet"][8:]] = manifest["dataSetSLIManifest"][
                            "mode"]
                else:
                    # single manifest
                    self.datasets[upload_info_json["dataSetSLIManifest"]["dataSet"][8:]] = \
                        upload_info_json["dataSetSLIManifest"]["mode"]
        except OSError as e:
            emsg = u"Error: Problem with etl working directory"
            logger.error(emsg, exc_info=True)
            raise gd.GoodDataError(emsg, e)
        except Exception as e:
            emsg = u"Error: Problem during parsing upload_info.json - please delete content of directory manifests"
            logger.error(emsg, exc_info=True)
            raise gd.GoodDataError(emsg, traceback.print_exc())

    def add_dataset(self, mode, dataset=None):
        if dataset:
            # individual dataset and mode will be set to self.datasets
            # TODO: check of dataset existence - now only blindly adding what was passed
            self.datasets[dataset] = mode
        else:
            # all datasets from project will beset for ETL using specified mode
            print "ALL"

    def prepare_upload(self):
        """ This function downloads manifest/s for given list of datasets from GoodData API
        1) modify names for csv columns to more human readable names
        2) save manifests in etl working directory in project/manifest
        3) save template csv with headers in etl working directory in project/csv
        4) creates final upload_info.json - in case of more datasets it creates batch mode manifest
        """
        # according to datasets we will download and consequently modify manifests for specified dataset
        headers = gd.http_headers_template.copy()
        headers["X-GDC-AuthTT"] = self.glo.generate_temporary_token()

        logger.debug("Preparing metadata for following datasets:\n{}".format(self.datasets))
        for dataset in self.datasets.keys():
            url = self.glo.gdhost + "/gdc/md/" + self.project + "/ldm/singleloadinterface/dataset." + dataset + "/manifest"
            request = urllib2.Request(url, headers=headers)

            try:
                logger.debug(gd.request_info(request))
                response = urllib2.urlopen(request)
            except urllib2.HTTPError as e:
                logger.error(e, exc_info=True)
                # for 40x errors we don't retry
                if e.code == 403 or e.code == 404:
                    raise gd.GoodDataAPIError(url, e, msg="Problem during retrieving a manifest")
                else:
                    raise Exception(e)
                    # to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry

            # processing of individual singleloadinterface/dataset response
            api_response = gd.check_response(response)
            logger.debug(gd.response_info(api_response))
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

        try:
            i = 0
            # writing manifests and csv templates files to etl working directory
            for dtset in self.datasets.keys():
                # write to file in dir manifests in etl working directory
                with open(os.path.join(self.wd, self.project, "manifests", dtset + ".json"), "w") as f:
                    f.write(json.dumps(self.manifests[i], sort_keys=True, indent=2, separators=(',', ': ')))
                # write csv file with template header to csv dir
                with open(os.path.join(self.wd, self.project, "csv", dtset + "_header.csv"), "w") as f:
                    pom = ""
                    for attr in self.csv_header_templates[i]:
                        pom += '"{}",'.format(attr)
                    f.write(pom[0:-1])
                i += 1

            # create final upload_info.json
            if len(self.datasets.keys()) > 1:  # we create SLI BATCH manifest
                upload_info_json = {"dataSetSLIManifestList": []}
                for manifest in self.manifests:
                    upload_info_json["dataSetSLIManifestList"].append(manifest)
            else:  # single manifest
                upload_info_json = dict(self.manifests[0])

            # write final upload_info.json to manifests directory
            with open(os.path.join(self.wd, self.project, "manifests", "upload_info.json"), "w") as f:
                f.write(json.dumps(upload_info_json, sort_keys=True, indent=2, separators=(',', ': ')))

        except OSError as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError("Problem with etl working directory", e)
        except IOError as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError("Problem during file operation", e)

    def perform_data_upload(self):
        """
        This method performs upload to user staging directory (WebDav)
        As this method can be also called directly(without calling prepare_upload() after creating GoodDataETL instance
        we have to check that all necessary files are in place.
        """
        if not self.datasets:
            emsg = "Error: You MUST specify datatests and upload modes - add datasets and run preparation phase again"
            logger.error("{}".format(emsg))
            raise gd.GoodDataError(emsg)

        # compare headers of csv files for upload with template csv files
        try:
            for dataset in self.datasets.keys():
                header_template_file = os.path.join(self.wd, self.project, "csv", dataset + "_header.csv")
                with open(header_template_file, "r") as f:
                    reader = csv.reader(f)
                    # csv header from template file
                    header_template = reader.next()
                header_csv_file = os.path.join(self.wd, self.project, "csv", dataset + ".csv")
                with open(header_csv_file, "r") as f:
                    reader = csv.reader(f)
                    # csv header from data file
                    header_csv = reader.next()

                header_template.sort()
                header_csv.sort()

                if header_template != header_csv:
                    raise gd.GoodDataError("Error: Header of template file and csv file for upload doesn't match")

        except IOError as e:
            emsg = "Problem during comparing csv headers - check that csv files are in csv folder within ETL working directory"
            logger.error("{}: {}".format(emsg, e))
            raise gd.GoodDataError("Error: {}".format(emsg), e)
        except gd.GoodDataError as e:
            emsg = "{}\n{}:\n{}\n{}:\n{}\nFile '{}' MUST contain same columns as file '{}' (order doesn't matter)".format(
                e,
                os.path.basename(header_template_file), header_template, os.path.basename(header_csv_file), header_csv,
                os.path.basename(header_csv_file), os.path.basename(header_template_file))
            logger.error(emsg)
            raise gd.GoodDataError("Error: {}".format(emsg))
        except Exception as e:
            emsg = "Unexpected problem during comparing csv headers"
            logger.error(emsg, exc_info=True)
            raise gd.GoodDataError("Error: {}".format(emsg), traceback.print_exc())

        # creating upload.zip
        try:
            # list of files for upload.zip
            files = []
            # adding csv files with data for upload
            for dataset in self.datasets.keys():
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
                    f.write("{}\n".format(info.filename))
                    f.write("\tModified:\t{}\n".format(datetime.datetime(*info.date_time)))
                    f.write("\tCompressed:\t{} bytes\n".format(info.compress_size))
                    f.write("\tUncompressed:\t{} bytes\n".format(info.file_size))
        except Exception as e:
            emsg = "Problem during creating upload.zip - check that all source files for upload are in csv directory"
            logger.error(emsg, exc_info=True)
            raise gd.GoodDataError("Error: {}".format(emsg), traceback.print_exc())

        # upload to WebDav
        upload_zip_size = os.path.getsize(os.path.join(self.wd, self.project, "upload.zip"))
        with open(os.path.join(self.wd, self.project, "upload.zip"), "rb") as f:
            headers = gd.http_headers_template.copy()
            headers["X-GDC-AuthTT"] = self.glo.generate_temporary_token()
            headers["Content-Type"] = "application/zip"
            headers["Content-Length"] = upload_zip_size

            self.remote_etl_dir = uuid.uuid4().hex
            url = "https://secure-di.gooddata.com/uploads/{}/{}/upload.zip".format(self.project, self.remote_etl_dir)
            request = urllib2.Request(url, headers=headers, data=f.read())
            request.get_method = lambda: 'PUT'

            try:
                logger.debug(gd.request_info(request))
                start_time = time.time()
                response = urllib2.urlopen(request)
                total_time_sec = time.time() - start_time
            except urllib2.HTTPError as e:
                logger.error(e)
                raise gd.GoodDataAPIError(url, e, msg="Problem during upload to GoodData WebDAV")
                # TODO: take a look at reason and in case of need check /gdc/ping anf if everything OK try retry
            except Exception as e:
                logger.error(e)
                raise Exception(e)

        with open(os.path.join(self.wd, self.project, "uploaded.to"), "w") as f:
            f.write("{}\n".format(url))

        # processing WebDav response
        api_response = gd.check_response(response)
        logger.debug(gd.response_info(api_response))
        logger.debug("File uploaded to WebDav in {}s".format(round(total_time_sec, 2)))

        response.close()

    def perform_project_load(self):

        headers = gd.http_headers_template.copy()
        headers["X-GDC-AuthTT"] = self.glo.generate_temporary_token()
        url = self.glo.gdhost + "/gdc/md/" + self.project + "/etl/pull2"
        etl_json = {"pullIntegration": self.project + "/" + self.remote_etl_dir}
        request = urllib2.Request(url, headers=headers, data=json.dumps(etl_json))

        try:
            logger.debug(gd.request_info(request))
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            # for 40x errors we don't retry
            if e.code == 404:
                logger.error(e, exc_info=True)
                raise gd.GoodDataAPIError(url, e, msg="GoodData project doesn't exist")
            elif e.code == 403:
                logger.error(e, exc_info=True)
                raise gd.GoodDataAPIError(url, e, msg="Admin or editor role is required")
            else:
                raise Exception(e)
                # to do take a look at reason and in case of need check /gdc/ping anf if everything OK try retry

        # 201 created - processing of etl/pull2 response
        api_response = gd.check_response(response)
        logger.debug(gd.response_info(api_response))
        poll_url = json.loads(api_response["body"])["pull2Task"]["links"]["poll"]

        # poll for full result
        url = self.glo.gdhost + poll_url
        etl_task_state = "RUNNING"

        logger.debug("Polling '{}' for final result".format(url))
        retry = 0
        while etl_task_state == "RUNNING":
            # current request
            request = urllib2.Request(url, headers=headers)
            try:
                response = urllib2.urlopen(request)
            except urllib2.HTTPError as e:
                if e.code == 401:
                    # it seems that a new temporary token should be generated and we need to repeat poll request
                    logger.debug("* 401 response on state of ETL task - calling for valid TT.")
                    headers["X-GDC-AuthTT"] = gl.generate_temporary_token()
                    continue
                else:
                    retry += 1
                    if retry == 4:
                        emsg = "Problem during call for state of ETL task"
                        logger.error("{}: {}".format(emsg, e))
                        raise gd.GoodDataAPIError(url, e, emsg)
                    else:
                        logger.warning(
                            "* {} response on state of ETL task - retry({}):\n{}".format(e.code, retry, e.read()))
            else:
                # poll response
                response_body = response.read()
                etl_task_state = json.loads(response_body)["wTaskStatus"]["status"]

            # don't spam wait some time
            time.sleep(3)

        # ETL finished - returning state and save text result message
        if etl_task_state == "ERROR":
            err_params = json.loads(response_body)["wTaskStatus"]["messages"][0]["error"]["parameters"]
            err_message = json.loads(response_body)["wTaskStatus"]["messages"][0]["error"]["message"]
            logger.error(err_message % (tuple(err_params)))
            self.etl_task_result = "ERROR" + err_message % (tuple(err_params))
            etl_ok = False
        elif etl_task_state == "OK":
            self.etl_task_result = etl_task_state
            etl_ok = True
        else:  # CANCELED ?
            self.etl_task_result = etl_task_state
            etl_ok = False

        logger.info("ETL has been finished with following state:\n{}".format(self.etl_task_result))
        return etl_ok

        # TODO - after ETL finish - download upload_status.json with details about ETL run


def create_dir_if_not_exists(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


# test code
if __name__ == "__main__":

    import logging.config

    # load the logging configuration
    logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
    logging.getLogger("root").setLevel(logging.DEBUG)
    logging.Logger.disabled = False

    # handlers
    console_handler = logging.getLogger().handlers[0]
    file_handler = logging.getLogger().handlers[1]
    # set log level for handlers - example
    console_handler.setLevel(logging.ERROR)
    file_handler.setLevel(logging.DEBUG)
    # example of removing handler
    logging.getLogger().removeHandler(console_handler)

    try:

        # GoodData login
        gl = gdlogin.GoodDataLogin("vladimir.volcko+sso@gooddata.com", "xxx")

        # Creating etl instance
        etl = GoodDataETL(gl, "gmlgncezgyatnnr0d1mc6tss82olgf0s", "/Users/VtG/Work/PycharmProjects/GD/etlwd")
        etl.add_dataset(u"INCREMENTAL", u"allgrain")
        etl.add_dataset(u"FULL", u"all")
        print(etl.datasets)

        # Preparing metadata for ETL
        etl.prepare_upload()

        # Time for custom code which somehow upload source csv files to csv directory in ETL working directory

        # Local data upload to GD WebDav server on the basis of metadata from prepare_upload()
        etl.perform_data_upload()

        # Main ETL - Loading data from Webdav(uploaded by perform_data_upload) to GD project
        if etl.perform_project_load():
            print("ETL for project '{}' finished with status {}".format(etl.project, etl.etl_task_result))
        else:
            print("ETL for project '{}' failed".format(etl.project))
            print("Error detail: {}".format(etl.etl_task_result))

        # GoodData logout
        gl.logout()

    except (gd.GoodDataError, gd.GoodDataAPIError) as e:
        print(e)
    except Exception, emsg:
        print(traceback.print_exc())
        print("General error: " + str(emsg))
