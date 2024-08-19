from flask import Flask
from flask_restful import Api
from modelos import db
from vistas import VistaEmployee, VistaDepartment, VistaJob

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
api.add_resource(VistaEmployee, '/employee')
api.add_resource(VistaDepartment, '/department')
api.add_resource(VistaJob, '/job')

# Ejecutar la aplicación solo si se ejecuta este archivo directamente
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
