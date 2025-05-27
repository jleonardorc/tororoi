#!/usr/bin/python
import cv2
import os
import pandas as pd
import datetime
import logging
import sys

def analyze_image(media_obj):
    media_obj_a = {}

    """Perform image analysis using OpenCV."""
    image = cv2.imread(os.path.join(media_obj['file_path'], media_obj['file_name']))
    print(image.shape)
    media_obj_a['contour'] = cv2.imencode('.jpg', image)[1].tobytes()
    
    # Face detection
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
    sys.exit()
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)

    # Collecting face location coordinates
    face_locations = [(x, y, w, h) for (x, y, w, h) in faces]  # List of tuples (x, y, width, height)

    media_obj_a['contains']['faces'] = str(face_locations) if face_locations else None

    media_obj_a['update_date'] = datetime.datetime.now()

    return media_obj_a

def get_media_id ():
    media_id = 123
    return media_id

def get_media_obj_by_id (media_id):
    media_obj = {'media_id' : media_id, 'file_path' : '/media/nas/MediaSSD/fotos/2010/201811', 'file_name' : '20181111_071846.jpg'}
    return media_obj

def analyze_batch():
    try:
        # Batch size for long-term processing
        batch_size = 1

        for x in range(batch_size):
            media_id = get_media_id()
            media_obj = get_media_obj_by_id(media_id)
            #mark_in_process(media_id)
    
            try:
                media_obj_analized = analyze_image(media_obj)
                print(media_obj_analized)
                logging.info(f"Successfully processed media_id: {media_id}")
            except Exception as e:
                error_message = str(e)
                logging.error(f"Failed processing media_id: {media_id}. Error: {error_message}")

    except Exception as e:
        logging.critical(f"Critical error in main process: {str(e)}")

def main():
    #Set PID file to avoid run duplicate
    run_path = os.path.abspath(os.path.dirname(__file__))
    pid_file = run_path  + "/tororoi_imganalize.pid"

    #do the magic
    analyze_batch()


if __name__ == "__main__":
    main()
