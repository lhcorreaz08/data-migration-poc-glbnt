from flask import Flask
from flask_restful import Api
from modelos import db
from vistas import *

app = Flask(__name__)


# Actualiza la cadena de conexión con la nueva contraseña
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://postgres:Ericsson99@34.69.86.242:5432/postgres'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


# Inicializar la base de datos
db.init_app(app)

# Crear todas las tablas necesarias
with app.app_context():
    db.create_all()

# Crear una instancia de Flask-RESTful API
api = Api(app)

# Registrar las rutas y sus vistas
api.add_resource(Pong, '/ping')

api.add_resource(VistaEmployee, '/employee')
api.add_resource(VistaDepartment, '/department')
api.add_resource(VistaJob, '/job')

api.add_resource(LoadHistEmployee, '/load-hist-employee')
api.add_resource(LoadHistJob, '/load-hist-job')
api.add_resource(LoadHistDepartment, '/load-hist-department')

api.add_resource(SnapshotEmployee, '/snapshot-employee')
api.add_resource(SnapshotDepartment, '/snapshot-department')
api.add_resource(SnapshotJob, '/snapshot-job')

api.add_resource(RestoreJobs, '/restore-jobs')
api.add_resource(RestoreDepartments, '/restore-departments')
api.add_resource(RestoreEmployees, '/restore-employees')

api.add_resource(EmployeesHiredByQuarter, '/employees-hired-by-quarter')
api.add_resource(DepartmentsAboveAverageHires, '/departments-above-average-hires')