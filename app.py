from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage
from datetime import datetime
from collections import defaultdict, Counter
import json
from typing import List, Tuple, Optional
import os

app = Flask(__name__)
CORS(app)

# Configurar el cliente de Google Cloud Storage
#storage_client = storage.Client()

# Get the environment variables
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG')

@app.route('/ping')
def hello_world():
    return 'Pong!'

