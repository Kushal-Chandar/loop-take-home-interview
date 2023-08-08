from flask import Flask, jsonify
import json
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS, cross_origin
from postgres import PostgresDatabase
import threading
from io import BytesIO
from process import ProcessRequest


app = Flask(__name__)
api = Api(app)
cors = CORS(app)
app.config["CORS_HEADERS"] = "Access-Control-Allow-Origin'"
db = PostgresDatabase()
db.runQuery(
    """
    CREATE TABLE IF NOT EXISTS report_status (
        report_id SERIAL PRIMARY KEY,
        status VARCHAR(10)
    )
    """
)

parser = reqparse.RequestParser()
parser.add_argument("report_id", type=int, location="args")


def generate_unique_report_id():
    db.runQuery("SELECT MAX(report_id) FROM report_status")
    result = db.fetchOne()
    return result[0] + 1 if (result and result[0]) else 1


def process_report(report_id):
    ProcessRequest(report_id)
    db.runQuery(
        f"UPDATE report_status SET status = 'Complete' WHERE report_id = {report_id}"
    )


class TriggerReport(Resource):
    def get(self):
        report_id = generate_unique_report_id()
        db.runQuery(
            f"INSERT INTO report_status (report_id, status) VALUES ({report_id}, 'Running')"
        )
        threading.Thread(target=process_report, args=(report_id,)).start()
        return {"report_id": report_id}


class GetReport(Resource):
    def get(self):
        args = parser.parse_args()

        report_id = args["report_id"]
        if not report_id:
            db.runQuery(f"SELECT COUNT(status) FROM report_status")
            report_count = db.fetchOne()
            if report_count and report_count[0]:
                return {"reports_count": report_count[0]}
            else:
                return {"message": "No reports found"}, 404

        db.runQuery(f"SELECT status FROM report_status WHERE report_id = {report_id}")
        status = db.fetchOne()

        if not (status and status[0]):
            return {"message": "Report not found"}, 404
        else:
            db.runQuery(f"SELECT report FROM reports WHERE report_id = {report_id}")
            file = db.fetchOne()
            if not (file and file[0]):
                return {"status": status[0]}
            response = json.loads(file[0])
            return {"status": status[0], "report": response}


api.add_resource(TriggerReport, "/trigger_report")
api.add_resource(GetReport, "/get_report")

if __name__ == "__main__":
    app.run(debug=True)
