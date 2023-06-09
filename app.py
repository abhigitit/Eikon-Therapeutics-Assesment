from flask import Flask, jsonify, request


app = Flask(__name__)

from pipelines import UserExperimentsPipeline
import config

app = Flask(__name__)
pipeline = None

pipeline = UserExperimentsPipeline()
@app.route("/start_etl", methods=["GET"])
def start_etl_process():

    try:
        pipeline.start_etl()
        return jsonify({"message": "ETL SUCCESS"})
    except Exception as e:
        print(e)
        return jsonify("ETL FAILED")

@app.route("/get_user_stats", methods=["GET"])
def get_user_stats():
    user_id = request.args.get("user_id")
    if user_id is None:
        return jsonify({"message": "user_id parameter is required"}), 400
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({"message": "user_id must be an integer"}), 400
    user_stats = pipeline.get_user_stats(user_id)
    if "message" in user_stats:
        return jsonify(user_stats), 400
    return jsonify(user_stats)




if __name__ == "__main__":
    
    app.run(host="0.0.0.0", port=config.PORT, debug=True)
