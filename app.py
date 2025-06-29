from flask import Flask, render_template
import importlib, os

app = Flask(__name__)

@app.route("/")
def home():
    strategy = importlib.import_module("strategy")
    results = strategy.scan_stocks()
    return render_template("index.html", stocks=results)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)