#|/usr/bin/python
import os
import sys
import traceback
import shutil
import exifread
import logging
import re
import enchant
import datetime
import json
import magic
import random
import string
import pathlib

import lib.common
from lib.tororoimediadb import MediaDB
from lib.tororoimedia import MediaObj

outputs_config = [ {"path": "/media/nas/MediaSSD/fotos/2000", "min_date": datetime.datetime.strptime("1977-01-01", '%Y-%m-%d'), "max_date": datetime.datetime.strptime("2009-12-31 23:59:59", '%Y-%m-%d %H:%M:%S')}
                 , {"path": "/media/nas/MediaSSD/fotos/2010", "min_date": datetime.datetime.strptime("2010-01-01", '%Y-%m-%d'), "max_date": datetime.datetime.strptime("2019-12-31 23:59:59", '%Y-%m-%d %H:%M:%S')}
                 , {"path": "/media/nas/MediaSSD/fotos/2020", "min_date": datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'), "max_date": datetime.datetime.strptime("2029-12-31 23:59:59", '%Y-%m-%d %H:%M:%S')}
                 ]

def run_etl_media(path):

    total = 0
    # Create DB object
    mdb = MediaDB(config)
    #Connect to db
    mdb.connect()
    for entry in os.scandir(path):

        """subdirs"""
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
            file_suffix = ((pathlib.Path(entry.path).suffix)[1:]).lower() 
        except OSError as error:
            logging.critical('Error calling is_dir(): %s', error)
            logging.debug(traceback.format_exc())
            continue

        if is_dir:
            logging.info("SubDir[%s]: %s", total, entry.path)
            total += run_etl_media(entry.path)
        elif file_suffix not in config['allowed_suffix']:
            # Is file type to be sorted
            #move to sort path
            logging.info(f'File {entry.path} not a media file moving to {config["sort_folder"]}')
            shutil.move(entry.path, os.path.join(config['sort_folder'], os.path.basename(entry.path)))
            continue
        else:
            #Increase number of analized files
            total += 1
            try:
                logging.info("File[%s]: %s", total, entry.path)
                #Extract media info from file
                logging.debug("Creating media metadata object from: %s" % entry.path)
                mo = MediaObj.fromfile(entry.path)
                logging.debug(f"created_on: {mo.data['created_on']}")
                if mo.data["created_on"] is None:
                    #move to sort path
                    logging.info(f'File {entry.path} unable to read info, moving to {config["sort_folder"]}')
                    shutil.move(entry.path, os.path.join(config['sort_folder'], os.path.basename(entry.path)))
                elif mo.data["file_name"].startswith("."):
                    #Hidden files, move to sort path
                    logging.info(f'File {entry.path} not a media file moving to {config["sort_folder"]}')
                    shutil.move(entry.path, os.path.join(config['sort_folder'], os.path.basename(entry.path)))
                else:
                    #Modify object to set the sorted path
                    a = mo.set_sorted_path(get_store_path(mo.data["created_on"]))
                    #Sort file
                    move_media(entry.path, mo)
                    #Insert record in db
                    mdb.media_insert(mo.get_data_insert())
                
            except Exception as error:
                logging.critical('Error sorting files: %s', error)
                logging.debug(traceback.format_exc())

        #count analized files and exit if more than limit
        if total >= config['max_analized_files']:
            logging.warning("Maximun analized files reached: %s", total)
            return total


    #db status
    logging.info("% s records in DB" % mdb.media_status())

    # Close DB connection
    mdb.disconnect()

    return total

def move_media(source_path, media):
    logging.debug("Request to move media " + media.data["file_path"] + "/" + media.data["file_name"])
    full_path = os.path.join(media.data["file_path"], media.data["file_name"])
    if os.path.exists(full_path):
        #if already exists check size
        dst_file_stat = os.stat(full_path)
        if media.data["file_size"] == dst_file_stat.st_size:
            logging.warning("Already exists with same size, removing source:" + source_path)
            os.remove(source_path)
            raise ValueError("Already exists in:" + full_path)
        else:
            chars=string.ascii_uppercase + string.digits
            opc = ''.join(random.choice(chars) for _ in range(4))
            oldname = media.data["file_name"]
            idx = oldname.index('.')
            media.data["file_name"] = oldname[:idx] + "_D" + opc + oldname[idx:]
            full_path = os.path.join(media.data["file_path"], media.data["file_name"])
            logging.warning("New size differ add, replace?")

    try:
        os.makedirs(media.data["file_path"],exist_ok=True)
        logging.info("Move " + source_path + " > " + full_path)
        #sys.exit()
        shutil.move(source_path, full_path)

    except OSError as error:
        logging.critical('Error reading files: %s', error, exc_info=True)
        raise ValueError("Cant move:" + source_path + " > " + full_path)


"""
Funtion to calculate the path form the config of serveral storage options inconfig outputs_config
"""
def get_store_path(file_date):
    store_path = None

    #Calculate path to store by range
    for o in outputs_config:
        try:
            if o['min_date'] <= file_date <= o['max_date']:
                store_path = o['path']
                logging.debug(f'Calculated path {store_path} for {file_date} from dates between {o["min_date"]} and {o["max_date"]}')
                break
        except:
            store_path = None

    if store_path is None:
        store_path = config['default_path']
        logging.debug("Calculated path %s from default", store_path)

    return store_path


def main():
    global config
    config = lib.common.configure()
    #Set PID file to avoid run duplicate
    run_path = os.path.abspath(os.path.dirname(__file__))
    pid_file = run_path  + "/tororoi.pid"
    lib.common.create_pid(pid_file)

    #do the magic
    logging.info("Extracting media in '% s'" % config['upload_folder'])
    total_extracted = run_etl_media(config['upload_folder'])
    logging.info("Extracted '% s' files" % total_extracted)
    lib.common.delete_empty_folders(config['upload_folder'])

    os.remove(pid_file)

if __name__ == "__main__":
    main()

