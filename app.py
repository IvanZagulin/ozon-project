from flask import Flask, request, render_template, redirect, url_for
from transfer import run_transfer, log_message, LOG_STORE
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        if "file" not in request.files:
            log_message("❗ Файл не был выбран")
            return redirect(request.url)
        file = request.files["file"]
        if file.filename == "":
            log_message("❗ Имя файла отсутствует")
            return redirect(request.url)
        if file:
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(filepath)
            run_transfer(filepath, log=log_message)
            return redirect(url_for("index"))

    return render_template("index.html", logs=LOG_STORE)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, threaded=True)

