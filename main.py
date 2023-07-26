from flask import Flask
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('report_id', type=int, location='args')

TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'},
}

class TriggerReport(Resource):
    def get(self):
        return {"report_id": 123}

class GetReport(Resource):
    def get(self):
        args = parser.parse_args()

        return {"report_id": args['report_id'],
                "status": "complete",
                "csv": TODOS,}


api.add_resource(TriggerReport, "/trigger_report")
api.add_resource(GetReport, "/get_report")

if __name__ == "__main__":
    app.run(debug=True)
