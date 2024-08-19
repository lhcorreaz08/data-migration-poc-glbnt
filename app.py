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

@app.route('/ping', methods=['GET'])
def healt_check():
    return 'Pong!'

def validate_hired_employees(data: List[dict]) -> Tuple[bool, str]:
    required_fields = {"id", "name", "datetime", "department_id", "job_id"}
    for row in data:
        if not isinstance(row, dict):
            return False, "Each entry must be a dictionary"
        if not required_fields.issubset(row.keys()):
            return False, "Missing required fields"
        try:
            datetime.fromisoformat(row['datetime'].replace("Z", "+00:00"))
            int(row['id'])
            int(row['department_id'])
            int(row['job_id'])
        except ValueError as e:
            return False, str(e)
    return True, "Valid data"

def validate_departments(data: List[dict]) -> Tuple[bool, str]:
    required_fields = {"id", "department"}
    for row in data:
        if not isinstance(row, dict):
            return False, "Each entry must be a dictionary"
        if not required_fields.issubset(row.keys()):
            return False, "Missing required fields"
        try:
            int(row['id'])
        except ValueError as e:
            return False, str(e)
    return True, "Valid data"

def validate_jobs(data: List[dict]) -> Tuple[bool, str]:
    required_fields = {"id", "job"}
    for row in data:
        if not isinstance(row, dict):
            return False, "Each entry must be a dictionary"
        if not required_fields.issubset(row.keys()):
            return False, "Missing required fields"
        try:
            int(row['id'])
        except ValueError as e:
            return False, str(e)
    return True, "Valid data"

@app.route('/employee', methods=['POST'])
def add_employee():
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({"error": "Data must be a list"}), 400
    
    is_valid, message = validate_hired_employees(data)
    if not is_valid:
        return jsonify({"error": message}), 400

    if len(data) > 1000:
        return jsonify({"error": "Batch size exceeds 1000"}), 400

    # Aquí se insertaría la lógica para guardar los datos en la base de datos o almacenamiento
    return jsonify({"message": "Employees added successfully"}), 201

@app.route('/department', methods=['POST'])
def add_department():
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({"error": "Data must be a list"}), 400
    
    is_valid, message = validate_departments(data)
    if not is_valid:
        return jsonify({"error": message}), 400

    if len(data) > 1000:
        return jsonify({"error": "Batch size exceeds 1000"}), 400

    # Aquí se insertaría la lógica para guardar los datos en la base de datos o almacenamiento
    return jsonify({"message": "Departments added successfully"}), 201

@app.route('/job', methods=['POST'])
def add_job():
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({"error": "Data must be a list"}), 400
    
    is_valid, message = validate_jobs(data)
    if not is_valid:
        return jsonify({"error": message}), 400

    if len(data) > 1000:
        return jsonify({"error": "Batch size exceeds 1000"}), 400

    # Aquí se insertaría la lógica para guardar los datos en la base de datos o almacenamiento
    return jsonify({"message": "Jobs added successfully"}), 201