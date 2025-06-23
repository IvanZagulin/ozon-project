import os
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/cards')
def cards():
    return render_template('cards.html')

@app.route('/logs')
def logs():
    return render_template('logs.html')

@app.route('/accounts')
def accounts():
    return render_template('accounts.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/import_export')
def import_export():
    return render_template('import_export.html')

@app.route('/maintenance')
def maintenance():
    return render_template('maintenance.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))  # Используется порт от Render
    app.run(host='0.0.0.0', port=port)        # Хост 0.0.0.0 нужен для Render
