from flask import Flask, request, jsonify
import os
from QuackingDuck import QuackingDuck
from openai import OpenAI

app = Flask(__name__) 

@app.route('/', methods=['GET', 'POST']) 
def get_sql():
    content = request.json
    question = content["question"]
    schema = content["schema"]

    client = OpenAI()
    quack = QuackingDuck(client)

    sql = quack.ask(question, schema, debug=True)

    return jsonify({"sql":sql})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)