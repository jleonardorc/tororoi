import requests
import yaml
import os
import sys
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename

import lib.common

app = Flask(__name__)
config = None

def upload_image(file, filename):
    url = config['server_ip']
    data = {'event': config['event_type'], 'device': request.user_agent.platform}
    return {'filename': filename, "url": url, "data" : data}

@app.route('/')
def home():
    return "<!DOCTYPE html><html><head><title>Tororoi Uploader</title><meta name=\"viewport\" content=\"width=device-width, initial-scale=1, maximum-scale=1\"></head><body style=\"font-family: sans-serif;\"><header><h1>Gracias por compartir</h1></header><section><form action=\"/upload\"  method=\"post\" enctype=\"multipart/form-data\"><div class=\"row\"><input type=\"file\" id=\"file\" name=\"file\" accept=\"image/*\" style=\"background-color:#eee;width:70%;padding:5px;\" multiple/><button style=\"border:0;background:black;color:white;width:25%;padding:7px;\" >Enviar</button></div></form></section><footer text-align=\"right\"><hr/><p align=\"right\">Tororoi</p></footer></body></html>"

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    files = request.files.getlist("file")
    response = []
    for file in files:
        #file = request.files['file']
        if not file.filename:
            return jsonify({'error': 'No selected file'})

        if file and file.content_type in config['image_types']:
            filename = secure_filename(file.filename)
            filepath = os.path.join(config['upload_folder'], filename)
            file.save(filepath)
            response.append(upload_image(file, filename))
        else:
            response.append({'filename': file.filename, 'error': 'Invalid file type'})

    return jsonify(response)

if __name__ == '__main__':
    config = lib.common.configure()
    #Set PID file to avoid run duplicate
    run_path = os.path.abspath(os.path.dirname(__file__))
    pid_file = run_path  + "/tororoi_uploader.pid"
    lib.common.create_pid(pid_file)
    #app.run(host='192.168.2.20', port=5000, debug=True, threaded=False)
    app.run(host='0.0.0.0', port=5000, debug=True)
    #app.run(host='0.0.0.0', port=5000, debug=False)
    os.remove(pid_file)

