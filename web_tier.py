from flask import Flask, request
import os
import random
import pandas as pd

app = Flask(__name__)

classification_file = 'classification_1000.csv'
classification_df = pd.read_csv(classification_file, index_col='Image')

@app.route('/', methods=['GET'])
def home():
    return 'Hello, World!', 200

@app.route('/', methods=['POST'])
def upload_file():
    if 'inputFile' not in request.files:
        return 'No file part', 400
    
    files = request.files['inputFile']
    
    if files.filename == '':
        return "Error: No file selected", 400
    
    name = os.path.splitext(files.filename)[0]
    try:
        result = classification_df.loc[name, 'Results']
    except IndexError:
        result = "Unknown"
    
    return f"{files.filename}:{result}", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)