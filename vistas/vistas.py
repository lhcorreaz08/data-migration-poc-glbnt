import fastavro
from google.cloud import storage
import csv
from io import StringIO, BytesIO
from flask import request, jsonify
from flask_restful import Resource
from modelos import db,  EmployeeSchema, DepartmentSchema, JobSchema, Employee, Department, Job
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
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
        
        

class SnapshotEmployee(Resource):
    def post(self):
        return self.generate_snapshot(Employee, EmployeeSchema, "employees")

    def generate_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"
        
        # Query all data from the table
        records = db.session.query(model_class).all()
        schema_instance = schema_class(many=True)
        data = schema_instance.dump(records)

        # Define Avro schema based on marshmallow schema
        avro_schema = {
            "type": "record",
            "name": table_name.capitalize(),
            "fields": []
        }

        for key, value in data[0].items():
            if isinstance(value, int):
                avro_schema['fields'].append({"name": key, "type": ["null", "int"]})
            elif isinstance(value, str):
                avro_schema['fields'].append({"name": key, "type": ["null", "string"]})
            else:
                raise ValueError(f"Unsupported data type: {type(value)} for field {key}")

        # Write data to Avro format in memory
        output = BytesIO()
        fastavro.writer(output, avro_schema, data)
        output.seek(0)

        # Create file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{base_path}{table_name}_snapshot_{timestamp}.avro"

        # Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_file(output, content_type="application/avro")
        
        return {"message": f"Snapshot for {table_name} saved as {file_name}"}, 200




class SnapshotDepartment(Resource):
    def post(self):
        return self.generate_snapshot(Department, DepartmentSchema, "departments")

    def generate_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"
        
        # Query all data from the table
        records = db.session.query(model_class).all()
        schema_instance = schema_class(many=True)
        data = schema_instance.dump(records)

        # Define Avro schema based on marshmallow schema
        avro_schema = {
            "type": "record",
            "name": table_name.capitalize(),
            "fields": []
        }

        for key, value in data[0].items():
            if isinstance(value, list):
                # Si es una lista, tratar como un array en Avro (por ejemplo, employee_ids)
                avro_schema['fields'].append({"name": key, "type": ["null", {"type": "array", "items": "int"}]})
            elif isinstance(value, int):
                avro_schema['fields'].append({"name": key, "type": ["null", "int"]})
            elif isinstance(value, str):
                avro_schema['fields'].append({"name": key, "type": ["null", "string"]})
            else:
                raise ValueError(f"Unsupported data type: {type(value)} for field {key}")

        # Write data to Avro format in memory
        output = BytesIO()
        fastavro.writer(output, avro_schema, data)
        output.seek(0)

        # Create file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{base_path}{table_name}_snapshot_{timestamp}.avro"

        # Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_file(output, content_type="application/avro")
        
        return {"message": f"Snapshot for {table_name} saved as {file_name}"}, 200


class SnapshotJob(Resource):
    def post(self):
        return self.generate_snapshot(Job, JobSchema, "jobs")

    def generate_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"
        
        # Query all data from the table
        records = db.session.query(model_class).all()
        schema_instance = schema_class(many=True)
        data = schema_instance.dump(records)

        # Define Avro schema based on marshmallow schema
        avro_schema = {
            "type": "record",
            "name": table_name.capitalize(),
            "fields": []
        }

        for key, value in data[0].items():
            if isinstance(value, list):
                # Handle list of integers
                avro_schema['fields'].append({"name": key, "type": ["null", {"type": "array", "items": "int"}]})
            elif isinstance(value, int):
                avro_schema['fields'].append({"name": key, "type": ["null", "int"]})
            elif isinstance(value, str):
                avro_schema['fields'].append({"name": key, "type": ["null", "string"]})
            else:
                raise ValueError(f"Unsupported data type: {type(value)} for field {key}")

        # Write data to Avro format in memory
        output = BytesIO()
        fastavro.writer(output, avro_schema, data)
        output.seek(0)

        # Create file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{base_path}{table_name}_snapshot_{timestamp}.avro"

        # Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_file(output, content_type="application/avro")
        
        return {"message": f"Snapshot for {table_name} saved as {file_name}"}, 200
    
    

class RestoreJobs(Resource):
    def post(self):
        return self.restore_from_snapshot(Job, JobSchema, "jobs")

    def restore_from_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Buscar el snapshot más reciente
        blobs = bucket.list_blobs(prefix=f"{base_path}{table_name}_snapshot_")
        latest_blob = max(blobs, key=lambda b: b.name)

        if not latest_blob:
            return {"message": f"No snapshot found for {table_name}"}, 404

        # Descargar y leer el archivo Avro
        avro_data = latest_blob.download_as_bytes()
        records = []
        with BytesIO(avro_data) as f:
            reader = fastavro.reader(f)
            for record in reader:
                records.append(record)

        # Truncar la tabla
        db.session.execute(text(f"TRUNCATE TABLE {model_class.__tablename__} RESTART IDENTITY CASCADE"))

        # Insertar los datos restaurados
        schema_instance = schema_class(many=True)
        objects = schema_instance.load(records, session=db.session)
        db.session.bulk_save_objects(objects)
        db.session.commit()

        return {"message": f"Restored {len(records)} records to {table_name} from snapshot {latest_blob.name}"}, 200


class RestoreDepartments(Resource):
    def post(self):
        return self.restore_from_snapshot(Department, DepartmentSchema, "departments")

    def restore_from_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Buscar el snapshot más reciente
        blobs = bucket.list_blobs(prefix=f"{base_path}{table_name}_snapshot_")
        latest_blob = max(blobs, key=lambda b: b.name)

        if not latest_blob:
            return {"message": f"No snapshot found for {table_name}"}, 404

        # Descargar y leer el archivo Avro
        avro_data = latest_blob.download_as_bytes()
        records = []
        with BytesIO(avro_data) as f:
            reader = fastavro.reader(f)
            for record in reader:
                records.append(record)

        # Truncar la tabla
        db.session.execute(text(f"TRUNCATE TABLE {model_class.__tablename__} RESTART IDENTITY CASCADE"))

        # Insertar los datos restaurados
        schema_instance = schema_class(many=True)
        objects = schema_instance.load(records, session=db.session)
        db.session.bulk_save_objects(objects)
        db.session.commit()

        return {"message": f"Restored {len(records)} records to {table_name} from snapshot {latest_blob.name}"}, 200




class RestoreEmployees(Resource):
    def post(self):
        return self.restore_from_snapshot(Employee, EmployeeSchema, "employees")

    def restore_from_snapshot(self, model_class, schema_class, table_name):
        bucket_name = "boot-caeb7_cloudbuild"
        base_path = "globant-challenges/avro_snapshots/"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Buscar el snapshot más reciente
        blobs = bucket.list_blobs(prefix=f"{base_path}{table_name}_snapshot_")
        latest_blob = max(blobs, key=lambda b: b.name)

        if not latest_blob:
            return {"message": f"No snapshot found for {table_name}"}, 404

        # Descargar y leer el archivo Avro
        avro_data = latest_blob.download_as_bytes()
        records = []
        with BytesIO(avro_data) as f:
            reader = fastavro.reader(f)
            for record in reader:
                records.append(record)

        # Truncar la tabla y reiniciar la secuencia de IDs
        db.session.execute(text(f"TRUNCATE TABLE {model_class.__tablename__} RESTART IDENTITY CASCADE"))

        # Insertar los datos restaurados, ignorando los IDs existentes si es necesario
        for record in records:
            record.pop('id', None)  # Elimina el campo 'id' para dejar que PostgreSQL asigne un nuevo ID automáticamente

        schema_instance = schema_class(many=True)
        objects = schema_instance.load(records, session=db.session)
        db.session.bulk_save_objects(objects)
        db.session.commit()

        return {"message": f"Restored {len(records)} records to {table_name} from snapshot {latest_blob.name}"}, 200




class EmployeesHiredByQuarter(Resource):
    def get(self):
        query = text("""
            SELECT
                d.department,
                j.job,
                SUM(CASE WHEN EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 1 THEN 1 ELSE 0 END) AS Q1,
                SUM(CASE WHEN EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2 THEN 1 ELSE 0 END) AS Q2,
                SUM(CASE WHEN EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 3 THEN 1 ELSE 0 END) AS Q3,
                SUM(CASE WHEN EXTRACT(QUARTER FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 4 THEN 1 ELSE 0 END) AS Q4
            FROM
                public.employees e
            JOIN
                public.departments d ON e.department_id = d.id
            JOIN
                public.jobs j ON e.job_id = j.id
            WHERE
                EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2021
            GROUP BY
                d.department, j.job
            ORDER BY
                d.department ASC, j.job ASC;
        """)
        
        result = db.session.execute(query)
        data = []
        
        for row in result:
            data.append({
                "department": row.department,
                "job": row.job,
                "Q1": row.q1,
                "Q2": row.q2,
                "Q3": row.q3,
                "Q4": row.q4,
            })
        
        return jsonify(data)
    
    
class DepartmentsAboveAverageHires(Resource):
    def get(self):
        query = text("""
        WITH department_hires AS (
            SELECT
                e.department_id AS id,
                d.department,
                COUNT(e.id) AS hired
            FROM
                employees e
            JOIN
                departments d ON e.department_id = d.id
            WHERE
                EXTRACT(YEAR FROM TO_TIMESTAMP(e.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2021
            GROUP BY
                e.department_id, d.department
        ),
        average_hires AS (
            SELECT
                AVG(hired) AS avg_hires
            FROM
                department_hires
        )
        SELECT
            dh.id,
            dh.department,
            dh.hired
        FROM
            department_hires dh,
            average_hires ah
        WHERE
            dh.hired > ah.avg_hires
        ORDER BY
            dh.hired DESC;
        """)
        
        result = db.session.execute(query)
        data = []
        
        for row in result:
            data.append({
                "id": row.id,
                "department": row.department,
                "hired": row.hired
            })
        
        return jsonify(data)