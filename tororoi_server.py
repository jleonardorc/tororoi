#!/usr/bin/python3
from concurrent import futures
import grpc
from grpc_reflection.v1alpha import reflection
import time
import os
import psycopg2
import socket
import urllib.parse
from psycopg2 import sql
from pathlib import Path
import logging
import traceback

import remoteServer.tororoi_pb2_grpc as pb2_grpc
import remoteServer.tororoi_pb2 as pb2

import lib.common

class TororoiService(pb2_grpc.tororoiServerServicer):

    def __init__(self, *args, **kwargs):
        #global config

        self.config = lib.common.configure()
        self.conn = self.connect()
        pass

    #TODO should not be used once implemented library Media
    def connect(self):
        #global conn
        try:
            #if not self.conn:
            self.conn = psycopg2.connect(
                  host     = self.config["database"]["host"]
                , dbname   = self.config["database"]["dbname"]
                , user     = self.config["database"]["user"]
                , password = self.config["database"]["password"]
                )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.exception('Error: %s', error)
            raise error

    def GetMediaList(self, request, context):

        r = pb2.MediaIdList()

        try:
            cursor = self.conn.cursor()
            sql = "SELECT media_id FROM app_media ORDER BY create_date DESC LIMIT 10"
            cursor.execute(sql)
            recordMediaIds = cursor.fetchall()
            for rmi in recordMediaIds:
                r.mediaIdList.append(str(rmi[0]))
        except (Exception, psycopg2.DatabaseError) as error:
            logging.debug(traceback.format_exc())

        logging.debug(r)
        return r

    #TODO use TororiMedia library
    def GetMediaDataDB(self, mediaId):
        mediaData = pb2.MediaData()
        try:
            if not self.conn:
                self.connect()
            sql = "SELECT * FROM app_media WHERE media_id = %s"
            cursor = self.conn.cursor()
            cursor.execute(sql, (mediaId,))
            recordMediaIds = cursor.fetchall()
            for rmi in recordMediaIds:
                mediaData.mediaId = str(rmi[0])
                mediaData.fileName = str(rmi[1])
                mediaData.filePath = str(rmi[2])
                mediaData.createdOn = str(rmi[3])
                mediaData.fileSize = str(rmi[4])
                mediaData.fileInfo = str(rmi[5])
                mediaData.fileType = str(rmi[6])
                mediaData.contains = str(rmi[7])
                mediaData.contour = str(rmi[8])
                mediaData.author = str(rmi[9])
                mediaData.event = str(rmi[10])
                mediaData.place = str(rmi[11])
                mediaData.lat = str(rmi[12])
                mediaData.lon = str(rmi[13])
                mediaData.emotion = str(rmi[14])
                mediaData.criteria = str(rmi[15])
                mediaData.tag = str(rmi[16])
                mediaData.subject = str(rmi[17])
                mediaData.description = str(rmi[18])
                mediaData.translation = str(rmi[19])
                mediaData.audio = str(rmi[20])
                mediaData.camera = str(rmi[21])
                mediaData.status = str(rmi[22])
                mediaData.channel = str(rmi[23])
                mediaData.createdBy = str(rmi[24])
                mediaData.createDate = str(rmi[25])
                mediaData.updatedBy = str(rmi[26])
                mediaData.updateDate = str(rmi[27])


        except (Exception, psycopg2.DatabaseError) as error:
            logging.debug(traceback.format_exc())
        logging.debug(mediaData)
        return mediaData

    def GetMediaDataById(self, request, context):
        mediaData = self.GetMediaDataDB(request.mediaId)
        return mediaData

    def GetMediaUrlById(self, request, context):
        mediaData = self.GetMediaDataDB(request.mediaId)
        p = Path(mediaData.filePath
                + "/" 
                + mediaData.fileName)
        new_path = str(p.relative_to(self.config["root_folder"]))
        mediaUrl = pb2.MediaUrl(mediaUrl="http://" 
                                + self.config["server_ip"]
                                + ":"
                                + str(self.config["server_media_port"])
                                + "/"
                                + urllib.parse.quote(new_path)
                               )
        logging.debug(mediaUrl)
        return mediaUrl

    def GetMediaById(self, request, context):
        chunk_size = 1024
        image_path = Path(__file__).resolve().parent.parent.joinpath('images/python-grpc.png')
        logging.debug(image_path)
        return 0
        with image_path.open('rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield MediaChunk(data=chunk)


    #TODO use TororiMedia library
    def UpdateMediaDataById(self, request, context):
        mediaData = pb2.MediaData()
        mediaData.mediaId = request.mediaId
        try:
            mediaUpdateData = {}
            if (request.mediaId and request.mediaId.isdigit()):
                mediaUpdateData["media_id"] = request.mediaId
            else:
                return mediaData
            if (request.filePath): #TODO validate
                mediaUpdateData["file_path"] = request.filePath
            if (request.author): #TODO validate
                mediaUpdateData["author"] = request.author
            if (request.event): #TODO validate
                mediaUpdateData["event"] = request.event
            if (request.place): #TODO validate
                mediaUpdateData["place"] = request.place
            if (request.updatedBy): #TODO validate
                mediaUpdateData["updated_by"] = request.updatedBy
            else:
                mediaUpdateData["updated_by"] = "TODO"

            updatable_fields = list(mediaUpdateData.keys())

            sql_query = sql.SQL("UPDATE app_media SET {data}, update_date = CURRENT_TIMESTAMP WHERE media_id = {media_id}").format(
                data=sql.SQL(', ').join(
                    sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in updatable_fields
                ),
                media_id=sql.Placeholder('media_id')
            )
            logging.debug(sql_query.as_string(self.conn))
            logging.debug(mediaUpdateData)
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.execute(sql_query, mediaUpdateData)
                    updated_rows = cur.rowcount
                    logging.info("Udpdated:", updated_rows)
                    cur.close()
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            logging.debug(traceback.format_exc())

        return mediaData

def serve():

    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_tororoiServerServicer_to_server(TororoiService(), server)
    SERVICE_NAMES = (
        pb2.DESCRIPTOR.services_by_name["tororoiServer"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    logging.info("rcpServer started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    #config = lib.common.configure()
    #Set PID file to avoid run duplicate
    run_path = os.path.abspath(os.path.dirname(__file__))
    pid_file = run_path  + "/tororoi_server.pid"
    lib.common.create_pid(pid_file)
    try:
        serve()
    finally:
        os.remove(pid_file)

