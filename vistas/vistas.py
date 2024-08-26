from google.cloud import storage
import csv
from io import StringIO
from flask import request
from flask_restful import Resource
from modelos import db,  EmployeeSchema, DepartmentSchema, JobSchema
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError
from datetime import datetime


employee_schema = EmployeeSchema()
department_schema = DepartmentSchema()
job_schema = JobSchema()

class VistaEmployee(Resource):
    def post(self):
        data = request.get_json()
        try:
            employees = employee_schema.load(data, many=True, session=db.session)
            db.session.bulk_save_objects(employees)
            db.session.commit()
            return {"message": "Employees added successfully"}, 201
        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400

class VistaDepartment(Resource):
    def post(self):
        data = request.get_json()
        try:
            departments = department_schema.load(data, many=True, session=db.session)
            db.session.bulk_save_objects(departments)
            db.session.commit()
            return {"message": "Departments added successfully"}, 201
        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400

class VistaJob(Resource):
    def post(self):
        data = request.get_json()
        try:
            jobs = job_schema.load(data, many=True, session=db.session)
            db.session.bulk_save_objects(jobs)
            db.session.commit()
            return {"message": "Jobs added successfully"}, 201
        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400
        
class Pong(Resource):
    def get(self):
        return {"message": "Pong!"}, 200
    
    
    
class LoadHistEmployee(Resource):
    def post(self):
        bucket_name = "boot-caeb7_cloudbuild"
        file_name = "globant-challenges/csv-hist/hired_employees.csv"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        # Parse CSV content
        csv_file = StringIO(content)
        reader = csv.reader(csv_file)

        try:
            employees = []
            for row in reader:
                # Convert datetime string to a Python datetime object
                employee_data = {
                    "id": int(row[0]),
                    "name": row[1],
                    "datetime": row[2], 
                    "department_id": int(row[3]) if row[3] else 1,
                    "job_id": int(row[4]) if row[4] else 1
                }
                employees.append(employee_data)

            employees = employee_schema.load(employees, many=True, session=db.session)
            db.session.bulk_save_objects(employees)
            db.session.commit()
            return {"message": "Employees loaded successfully"}, 201

        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400
        
        
        

class LoadHistJob(Resource):
    def post(self):
        bucket_name = "boot-caeb7_cloudbuild"
        file_name = "globant-challenges/csv-hist/jobs.csv"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        # Parse CSV content
        csv_file = StringIO(content)
        reader = csv.reader(csv_file)

        try:
            jobs = [{"id": int(row[0]), "job": row[1]} for row in reader]
            jobs = job_schema.load(jobs, many=True, session=db.session)
            db.session.bulk_save_objects(jobs)
            db.session.commit()
            return {"message": "Jobs loaded successfully"}, 201

        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400
        
        


class LoadHistDepartment(Resource):
    def post(self):
        bucket_name = "boot-caeb7_cloudbuild"
        file_name = "globant-challenges/csv-hist/departments.csv"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        # Parse CSV content
        csv_file = StringIO(content)
        reader = csv.reader(csv_file)

        try:
            departments = [{"id": int(row[0]), "department": row[1]} for row in reader]
            departments = department_schema.load(departments, many=True, session=db.session)
            db.session.bulk_save_objects(departments)
            db.session.commit()
            return {"message": "Departments loaded successfully"}, 201

        except IntegrityError:
            db.session.rollback()
            return {"message": "Integrity error occurred"}, 409
        except ValidationError as err:
            return err.messages, 400