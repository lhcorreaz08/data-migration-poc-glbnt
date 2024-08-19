from flask_sqlalchemy import SQLAlchemy
from marshmallow import fields, Schema
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

db = SQLAlchemy()

# Modelo para Employees
class Employee(db.Model):
    __tablename__ = 'employees'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    datetime = db.Column(db.DateTime, nullable=False)
    department_id = db.Column(db.Integer, db.ForeignKey('departments.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)

# Modelo para Departments
class Department(db.Model):
    __tablename__ = 'departments'
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(128), nullable=False)
    employees = db.relationship('Employee', backref='department', lazy=True, cascade='all, delete, delete-orphan')

# Modelo para Jobs
class Job(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(128), nullable=False)
    employees = db.relationship('Employee', backref='job', lazy=True, cascade='all, delete, delete-orphan')

# Esquemas para serialización y deserialización
class EmployeeSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Employee
        include_relationships = True
        load_instance = True

class DepartmentSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Department
        include_relationships = True
        load_instance = True

class JobSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Job
        include_relationships = True
        load_instance = True
