import os
import threading
import queue
import io
import time
from flask import Flask, render_template, request, redirect, url_for, Response, send_file, jsonify
import glob
import pandas as pd
from transfer import run_transfer, log_message, LOG_STORE

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/cards')
def cards():
    return render_template('cards.html')

@app.route('/logs')
def logs():
    files = sorted(glob.glob('logs_data/*.json'), reverse=True)
    names = [os.path.basename(f) for f in files]
    return render_template('logs.html', files=names)

@app.route('/logs/download/<name>')
def download_log(name):
    path = os.path.join('logs_data', name)
    df = pd.read_json(path)
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=name.replace('.json','.xlsx'))

@app.route('/accounts')
def accounts():
    return render_template('accounts.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/import_export', methods=['GET','POST'])
def import_export():
    if request.method == 'POST':
        f = request.files.get('file')
        if f:
            print("[ЗАГРУЗКА] Файл получен:", f.filename)
            path = os.path.join('uploads', f.filename)
            os.makedirs('uploads', exist_ok=True)
            f.save(path)
            threading.Thread(target=run_transfer, args=(path, log_message), daemon=True).start()
        return render_template('import_export.html')
    return render_template('import_export.html')

@app.route('/logs_data')
def logs_data():
    return jsonify(LOG_STORE)

@app.route('/maintenance')
def maintenance():
    return render_template('maintenance.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
