import os
import sys
import requests
import logging
import json
import tarfile

cloudant_user = os.getenv('CLOUDANT_USERNAME', 'admin')
cloudant_pass = os.getenv('CLOUDANT_PASSWORD', 'pass')
cloudant_url = "http://" + cloudant_user + ":" + cloudant_pass + "@cloudant-svc/"

cur_dir = os.path.dirname(os.path.realpath(__file__))
out_dir = cur_dir + '/cloudant-backup'
out_file = cur_dir + '/cloudant-backup.tar'
log_file = cur_dir + '/cloudant-backup.log'

all_docs_suffix = '/_all_docs?include_docs=true'
excluded_DBs = ["metrics", "portal-common-api_wdp_private_cloud", "notebook_api_wdp_private_cloud", "community_content"]

log_level = logging.INFO
import_or_export = ""

def usage():
    print("""
This utility provides the capability to export from or import into the cloudant database.

The export process will produce the tar file 'cloudant-backup.tar' in the same directory as this script, that would contain a directory of json files which correspond to each exported database.
To export from cloudant: 
    python {} export [--debug]

The import process expects the tar file 'cloudant-backup.tar' that contains a directory of json files which correspond to each exported database, in the same directory as this script.
To import into cloudant:
    python {} import [--debug]

For debug logging, run the script with the optional parameter '--debug'.
""".format(__file__,__file__))
    sys.exit(2)

def parseArgs(argv):
    global import_or_export, log_level

    for curr_arg in argv:
        if curr_arg in ("--help") or curr_arg in ("-h"):
            usage()
        elif curr_arg in ("--debug"):
            log_level = logging.DEBUG
        elif curr_arg in ("export"):
            if import_or_export != "":
                print("\nERROR: sub-command must be either 'export' or 'import'")
                usage()
            import_or_export = "export"
        elif curr_arg in ("import"):
            if import_or_export != "":
                print("\nERROR: sub-command must be either 'export' or 'import'")
                usage()
            import_or_export = "import"
        else:
            print("\nERROR: unrecognized argument: " + curr_arg)
            usage()

    if import_or_export == "":
        print("\nERROR: missing sub-command 'export' or 'import'")
        usage()

# checks connection to cloudant server by doing simple GET call
def checkLiveliness():
    status_code = 200
    logging.info("Checking that cloudant server is up")

    try:
        resp = requests.get(cloudant_url)
        data = resp.json()
        status_code = resp.status_code
        logging.debug("Returned status code: " + str(status_code))

    except requests.exceptions.RequestException as e:
        logging.error("Exception caught when connecting to cloudant server")
        logging.error(e)
        status_code = 503

    if status_code > 299:
        logging.error("Connecting to cloudant server returned status code: " + str(status_code))
        sys.exit(1)

    logging.info("Cloudant server is up")

# query cloudant for list of databases, return json data
def queryDatabases():
    query = cloudant_url + "_all_dbs"
    logging.info("Querying databases")

    try:
        resp = requests.get(url=query)
        data = resp.json()
        status_code = resp.status_code
        logging.debug("Returned status code: " + str(status_code))

    except requests.exceptions.RequestException as e:
        logging.error("Exception caught when querying cloudant server")
        status_code = 503

    if status_code > 299:
        logging.error("Connecting to cloudant server returned status code: " + str(status_code))
        sys.exit(1)

    logging.debug(data)

    return data

# specific function for 'privatecloud-users' database which prints user info
def printUsersInfo(data):
    num_users = str(len(data['rows']))
    logging.info("Found " + num_users  + " users:")

    for user_doc in data['rows']:
        user_id = user_doc['id']
        uniq_id = user_doc['doc']['uid']
        logging.info("user " + user_id + " with uid " + uniq_id)

# iterate through list of databases given in json
# do get call to get json of database
# write json to file
def exportDBs(databases):
    logging.info("Exporting databases")

    all_dbs_success = True

    for database in databases:
        if database.startswith('_') or database in excluded_DBs:
            logging.debug("Skipping " + database)
            continue

        query = cloudant_url + database + all_docs_suffix
        logging.info("Exporting database " + database)

        try:
            resp = requests.get(url=query)
            status_code = resp.status_code
            data = resp.json()
            logging.debug("Returned status code: " + str(status_code))

        except requests.exceptions.RequestException as e:
            logging.error("Exception caught when exporting from cloudant server")
            logging.error(e)
            all_dbs_success = False
            continue

        if status_code > 299:
            logging.error("Connecting to cloudant server returned status code: " + str(status_code))
            all_dbs_success = False
            continue
        if 'error' in data:
            logging.warn(database + " contains error, not exporting")
            continue
        if data['total_rows'] == 0:
            logging.warn(database + " is zero-sized, not exporting")
            continue

        if database == 'privatecloud-users':
            printUsersInfo(data) 

        wrapped_json = json.loads('{"docs":' + json.dumps(data) + '}')

        logging.debug("Writing database " + database + " to json file")
        with open(out_dir + '/' + database + '.json', 'wb') as f:
            json.dump(wrapped_json, f, ensure_ascii=False)

    if not all_dbs_success:
        logging.error("Failed to export at least one database")
        sys.exit(1)

# archive the produced directory of json files
def archiveDirectory(databases):
    logging.info("Creating tar archive: " + out_file)
    tar = tarfile.open(out_file, "w")
    tar.add(out_dir,arcname='cloudant-backup')
    tar.close()

    logging.debug("Removing exported directory")
    for database in databases:
        if os.path.exists(out_dir + '/' + database + '.json'):
            os.remove(out_dir + '/' + database + '.json')
    os.rmdir(out_dir)

    if not os.path.exists(out_file):
        logging.error("Failed to produce tar file " + out_file)
        sys.exit(1)

# export sub-command entrypoint
def runExport():
    if os.path.exists(out_dir):
        if not os.path.isdir(out_dir):
            logging.error(out_dir + " exists as a file, please move or remove it")
            sys.exit(1)
    else:
        logging.debug("Creating backup directory: " + out_dir)
        os.makedirs(out_dir)

    checkLiveliness()
    databases = queryDatabases()
    exportDBs(databases)
    archiveDirectory(databases)

    logging.info("Export process complete, resulting archive is " + out_file)

# given database name and its data as json, upload it by doing a POST call
def bulkUploadDB(database_name, data):
    logging.info("Uploading to database " + database_name)

    query = cloudant_url + database_name + '/_bulk_docs'
    headers = {'content-type':'application/json; charset=utf-8'}
    data_array = []

    # required to delete the revision key to allow it to re-generate one in cloudant
    for doc in data['docs']['rows']:
        data_doc = doc['doc']
        del data_doc['_rev']
        data_array.append(data_doc)

    final_data_array = {'docs':data_array}

    try:
        resp = requests.post(url=query,headers=headers,json=final_data_array)
        status_code = resp.status_code
        logging.debug("Returned status code: " + str(status_code))

    except requests.exceptions.RequestException as e:
        logging.error("Exception caught when importing to cloudant server")
        logging.error(e)
        return False

    if status_code != 200 and status_code != 201:
        logging.error("Non-successful status code: " + status_code)
        return False

    return True

# given database name, delete the database and its contents, and re-create it
def recreateDB(database_name):
    logging.info("Deleting database " + database_name)

    query = cloudant_url + database_name
    headers = {'content-type':'application/json; charset=utf-8'}

    try:
        resp = requests.delete(url=query)
        status_code = resp.status_code
        logging.debug("Returned status code: " + str(status_code))

    except requests.exceptions.RequestException as e:
        logging.error("Exception caught when deleting database in cloudant server")
        logging.error(e)
        return False

    logging.info("Creating database " + database_name)

    try:
        resp = requests.put(url=query,headers=headers)
        status_code = resp.status_code
        logging.debug("Returned status code: " + str(status_code))

    except requests.exceptions.RequestException as e:
        logging.error("Exception caught when creating database in cloudant server")
        logging.error(e)
        return False

    if status_code != 200 and status_code != 201:
        logging.error("Non-successful status code: " + status_code)
        return False

    return True

# iterate through files in extracted directory, and call recreateDB() and bulkuploadDB()
def importDBs():
    all_dbs_success = True

    for database_file in os.listdir(out_dir):
        if not database_file.endswith('.json'):
            logging.warn("File " + database_file + " is not a json file, not importing")
            continue

        logging.debug("Opening " + out_dir + '/' + database_file)
        with open(out_dir + '/' + database_file) as f:
            data = json.load(f)
            logging.debug(json.dumps(data, indent=4, sort_keys=True))

        database_name = database_file[:-5]
        logging.debug("Database name: " + database_name)

        success = recreateDB(database_name)
        if not success:
            all_dbs_success = False
            continue

        success = bulkUploadDB(database_name, data)
        if not success:
            all_dbs_success = False
            continue

    if not all_dbs_success:
        logging.error("Failed to create or upload to at least one database")
        sys.exit(1)

# extract the tar archive
def extractArchive():
    logging.info("Extracting tar archive: " + out_file)
    try:
        tar = tarfile.open(out_file)

    except tarfile.ReadError as e:
        logging.error("Failed to read tar file " + out_file)
        sys.exit(1)

    try:
        tar.extractall(path=cur_dir)

    except tarfile.ExtractError as e:
        logging.error("Failed to extract tar file" + out_file)
        sys.exit(1)

    tar.close()

    if not os.path.exists(out_dir):
        logging.error("Extracting tar file did not result in: " + out_dir)
        sys.exit(1)    

# import sub-command entrypoint
def runImport():
    if not os.path.exists(out_file):
        logging.error("Unable to find cloudant tar archive 'cloudant-backup.tar'")
        usage()

    checkLiveliness()
    extractArchive()
    importDBs()

    logging.info("Import process complete")

# prepare logging
def createLogger():
    logger = logging.getLogger()
    logger.setLevel(log_level)

    fh = logging.FileHandler(log_file)
    ch = logging.StreamHandler()

    # do not print timestamps in stdout, only in log file
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    fh.setFormatter(logging.Formatter('%(asctime)s -- %(levelname)s: %(message)s'))

    logger.addHandler(ch)
    logger.addHandler(fh)

    # if not debug logging, create a null handler for "requests" module as it fills up the log
    if log_level == logging.INFO:
        requests_log = logging.getLogger("requests")
        requests_log.addHandler(logging.NullHandler())
        requests_log.propagate = False

if __name__ == "__main__":
    parseArgs(sys.argv[1:])

    createLogger()

    if import_or_export == 'export':
        runExport()
    elif import_or_export == 'import':
        runImport()

    logging.info("Log file with timestamps can be found at: " + log_file)

    sys.exit(0)
