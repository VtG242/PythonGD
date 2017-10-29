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
        self.datasets = []
        self.manifests = []
        self.csv_header_templates = []

        # creating of a working_directory
        try:
            create_dir_if_not_exists(self.wd)
        except OSError as e:
            raise gd.GoodDataError("Problem with etl working directory", e)

    def prepare_upload(self, datasets):
        """ This function downloads manifest/s for given list of datasets from GoodData API
        1) modify names for csv columns to more human readable names
        2) save manifests in etl working directory in project/manifest
        3) save template csv with headers in etl working directory in project/csv
        4) creates final upload_info.json - in case of more datasets it creates batch mode manifest
        """
        self.datasets = datasets
        self.datasets.sort()

        # according to datasets we will download and consequently modify manifests for specified dataset
        headers = gd.http_headers_template.copy()
        headers["X-GDC-AuthTT"] = etl.glo.generate_temporary_token()

        for dataset in self.datasets:
            url = etl.glo.gdhost + "/gdc/md/" + self.project + "/ldm/singleloadinterface/dataset." + dataset + "/manifest"
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

        # writing manifests and csv templates files to etl working directory
        try:
            # if directory infrastructure doesn't exist let's create it
            create_dir_if_not_exists(os.path.join(self.wd, self.project))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "csv"))
            create_dir_if_not_exists(os.path.join(self.wd, self.project, "manifests"))

            # write to file in dir manifests in etl working directory
            for i in range(len(self.datasets)):
                with open(os.path.join(self.wd, self.project, "manifests", self.datasets[i] + ".json"),
                          "w") as f:
                    f.write(json.dumps(self.manifests[i], sort_keys=True, indent=2, separators=(',', ': ')))
            # write csv file with template header to csv dir
            for i in range(len(self.datasets)):
                with open(os.path.join(self.wd, self.project, "csv", self.datasets[i] + "_header.csv"), "w") as f:
                    pom = ""
                    for attr in self.csv_header_templates[i]:
                        pom += '"{}",'.format(attr)
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
            logger.error(e, exc_info=True)
            raise gd.GoodDataError("Problem with etl working directory", e)
        except IOError as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError("Problem during file operation", e)

    def perform_upload(self):
        """
        This method performs upload to user staging directory (WebDav)
        As this method can be also called directly(without calling prepare_upload() after creating GoodDataETL instance
        we have to check that all necessary files are in place.
        """
        if not self.datasets:
            try:
                with open(os.path.join(self.wd, self.project, "manifests", "upload_info.json"), "r") as f:
                    upload_info_json = json.loads(f.read())

                if upload_info_json.has_key("dataSetSLIManifestList"):
                    for manifest in upload_info_json["dataSetSLIManifestList"]:
                        self.datasets.append(manifest["dataSetSLIManifest"]["dataSet"][8:])
                else:
                    self.datasets.append(upload_info_json["dataSetSLIManifest"]["dataSet"][8:])
            except Exception as e:
                logger.error(e, exc_info=True)
                raise gd.GoodDataError("Problem parsing upload_info.json - run preparation phase and try it again", e)

        # compare headers of csv files for upload with template csv files
        try:
            p = 0
            for dataset in self.datasets:
                header_template_file = os.path.join(self.wd, self.project, "csv", dataset + "_header.csv")
                with open(header_template_file, "r") as f:
                    reader = csv.reader(f)
                    header_template = reader.next()
                header_csv_file = os.path.join(self.wd, self.project, "csv", dataset + ".csv")
                with open(header_csv_file, "r") as f:
                    reader = csv.reader(f)
                    header_csv = reader.next()

                header_template.sort()
                header_csv.sort()

                if header_template != header_csv:
                    raise gd.GoodDataError("Error: Header of template file and csv file for upload doesn't match")
                p += 1
            else:
                # it should not happen in normal circumstances - I used this only for testing :-)
                if p == 0:
                    raise Exception("Checking of csv headers did not happen (self.datasets empty)")

        except IOError as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError("IO problem during comparing csv headers", e)
        except gd.GoodDataError as e:
            emsg = "{}\n{}:\n{}\n{}:\n{}\nFile '{}' MUST contain same columns as file '{}' (order doesn't matter)".format(
                e,
                os.path.basename(header_template_file), header_template, os.path.basename(header_csv_file), header_csv,
                os.path.basename(header_csv_file), os.path.basename(header_template_file))
            logger.error(emsg)
            raise gd.GoodDataError(emsg)
        except Exception as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError(e)

        # creating upload.zip
        try:
            # list of files for upload.zip
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
                    f.write("{}\n".format(info.filename))
                    f.write("\tModified:\t{}\n".format(datetime.datetime(*info.date_time)))
                    f.write("\tCompressed:\t{} bytes\n".format(info.compress_size))
                    f.write("\tUncompressed:\t{} bytes\n".format(info.file_size))
        except Exception as e:
            logger.error(e, exc_info=True)
            raise gd.GoodDataError(
                "Problem during creating upload.zip - check that all source files for upload are in csv directory", e)

        # upload to WebDav
        upload_zip_size = os.path.getsize(os.path.join(self.wd, self.project, "upload.zip"))
        with open(os.path.join(self.wd, self.project, "upload.zip"), "rb") as f:
            headers = gd.http_headers_template.copy()
            headers["X-GDC-AuthTT"] = self.glo.generate_temporary_token()
            headers["Content-Type"] = "application/zip"
            headers["Content-Length"] = upload_zip_size

            remote_etl_dir = uuid.uuid4().hex
            url = "https://secure-di.gooddata.com/uploads/{}/{}/upload.zip".format(self.project, remote_etl_dir)
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

        # Preparing metadata for ETL
        etl.prepare_upload(["allgrain", "all"])

        # Time for custom code which somehow upload source csv files to csv directory in ETL working directory

        # Main upload to GoodData platform
        etl.perform_upload()

        # GoodData logout
        gl.logout()
    except (gd.GoodDataError, gd.GoodDataAPIError) as e:
        print(e)
    except Exception, emsg:
        print(traceback.print_exc())
        print("General error: " + str(emsg))
