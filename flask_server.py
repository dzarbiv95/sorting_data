import connection_db as db
import setup
from flask import Flask
from flask import request
from flask import json
app = Flask(__name__)


@app.route("/API/getAds/")
def get_ads():
    top = request.args.get('top', default=None, type=int)
    data = db.fetch_first_words(db.connection(), setup.RESULTS_STEP1_TABLE_NAME, top)
    print(len(data))
    return json.dumps([row[0] for row in data])


if __name__ == "__main__":
    app.run('0.0.0.0', 8080)