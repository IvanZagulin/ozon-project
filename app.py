from flask import Flask, request, render_template, redirect, url_for, send_from_directory
from transfer import run_transfer, log_message, LOG_STORE
import os
from werkzeug.utils import secure_filename

# ───────────────────── Config ───────────────────────────────────
UPLOAD_FOLDER = "uploads"
LOG_DIR       = "logs_data"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# ───────────────────── Routes ───────────────────────────────────
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            log_message("❗ Файл не был выбран или имя файла отсутствует")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        # запуск импорта
        run_transfer(filepath)
        return redirect(url_for("index"))

    return render_template("index.html", logs=LOG_STORE)

@app.route("/logs")
def logs():
    files = sorted(os.listdir(LOG_DIR))
    return render_template("logs.html", files=files)

@app.route("/logs/download/<path:filename>")
def download_log(filename):
    return send_from_directory(LOG_DIR, filename, as_attachment=True)

# ───────────────────── Entrypoint ───────────────────────────────
if __name__ == "__main__":
    # debug=True только для локальной разработки
    app.run(host="0.0.0.0", port=8000, debug=True, threaded=True)
