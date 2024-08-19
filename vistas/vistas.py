from flask import request, jsonify
from flask_restful import Resource
from modelos import db, Employee, Department, Job, EmployeeSchema, DepartmentSchema, JobSchema
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError

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