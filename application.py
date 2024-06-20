from flask import Flask, request, jsonify
import matplotlib
from flask_cors import CORS
from Execute.execute import forecast_pipeline
from Execute.execute_hyperparameters import  tuning
import threading

application = Flask(__name__)
cors = CORS(application)
matplotlib.use("Agg")


def format_date(date):
    return date.strftime('%Y-%m-%d')


@application.route('/forecast', methods=['POST'])
def forecast():
    try:
        data = request.json
        commodity_name = data.get('commodity_name')
        forecast_pipeline(commodity_name)
        return jsonify({"message": "Forecasting started for " + commodity_name}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route('/hyperparameter', methods=['POST'])
def hyperparameter():
    try:
        data = request.json
        commodity_name = data.get('commodity_name')
        tuning(commodity_name)
        return jsonify({"message": "Forecasting started for " + commodity_name}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    application.run(host="0.0.0.0", debug=True)
