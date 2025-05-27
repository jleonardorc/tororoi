#|/usr/bin/python
import os
import psycopg2
import sys
import logging
import datetime

import lib.common
from lib.tororoimediadb import MediaDB
from lib.tororoimedia import MediaObj

def get_all_files_by_directory(directory):
    """List all files in the given directory and subdirectories."""
    files = []
    for root, _, filenames in os.walk(directory):
        filenames = [f for f in filenames if not f[0] == '.']
        #_[:] = [d for d in _ if not d[0] == '.']
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def get_all_db_files_by_directory(directory):
    """
    List all records of media files in db for the given directory and subdirectories
    """
    files = []
    try:
        # Create DB object
        mdb = MediaDB(config)
        #Connect to db
        mdb.connect()
        # Get the list of files from the database
        files = mdb.get_files_from_db(directory)
        mdb.disconnect()
    except Exception as error:
        logging.exception("Error getting files from db: {e}".format(e=error))

    return files

def main(folder_path):

    global config
    config = lib.common.configure()
    #Set PID file to avoid run duplicate
    run_path = os.path.abspath(os.path.dirname(__file__))
    pid_file = run_path  + "/tororoi_folder2db.pid"
    lib.common.create_pid(pid_file)

    logging.info("Extracting media in '% s'" % folder_path)

    # Get the list of files in the directory and subdirectories
    folder_files = get_all_files_by_directory(folder_path)

    # Get info from database
    db_files = get_all_db_files_by_directory(folder_path)

    try:
        # Determine the lists
        registered_files = db_files
        files_not_in_db = [f for f in folder_files if f not in db_files]
        files_in_db_and_folder = [f for f in folder_files if f in db_files]
        files_in_db_not_in_folder = [f for f in db_files if f not in folder_files]

        # Action for each list
        logging.info("%s Files records in the database with that path" % len(registered_files))
        for file in registered_files:
            logging.debug(file)

        logging.info("%s Files in the folder but not registered in the database" % len(files_not_in_db))
        # Create DB object
        mdb = MediaDB(config)
        #Connect to db
        mdb.connect()
        #Loop over detected files and record in DB
        for file in files_not_in_db:
            logging.info("Extracting metadata from: %s" % file)
            mo = MediaObj.fromfile(file)
            logging.debug(mo.data)
            #insert record in db
            try:
                mdb.media_insert(mo.get_data_insert())
            except psycopg2.Error as e:
                if e.pgcode == '23505':
                    logging.info("File %s duplicated in DB, not inserted" % file)
                else:
                    raise e
        mdb.disconnect()

        logging.info("%s Files in the folder and registered in the database" % len(files_in_db_and_folder))
        for file in files_in_db_and_folder:
            logging.debug(file)

        logging.info("%s Files in the database but not in the folder" % len(files_in_db_not_in_folder))
        for file in files_in_db_not_in_folder:
            logging.debug(file)

    except Exception as error:
        logging.exception("Error in main: {e}".format(e=error))

    os.remove(pid_file)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tororoi_folder2db.py <image_folder_path>")
        sys.exit(1)
    if not os.path.isdir(sys.argv[1]):
        print("Folder does not exists")
        sys.exit(1)

    folder_path = os.path.normpath(sys.argv[1])

    main(folder_path)

