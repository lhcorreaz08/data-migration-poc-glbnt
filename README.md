# Flask RESTful API for Employee Management

## Descripción

Esta es una aplicación Flask RESTful API que maneja datos de empleados, departamentos y trabajos. La API permite cargar datos históricos desde archivos CSV, crear snapshots en formato Avro, restaurar datos desde esos snapshots y realizar consultas específicas sobre los datos.

## Características

- **Cargar datos históricos**: Endpoints para cargar datos históricos de empleados, departamentos y trabajos desde archivos CSV almacenados en Google Cloud Storage.
- **Crear snapshots**: Endpoints para crear snapshots de los datos actuales en formato Avro y almacenarlos en Google Cloud Storage.
- **Restaurar desde snapshots**: Endpoints para restaurar los datos desde snapshots Avro.
- **Consultas avanzadas**: Endpoints para consultar empleados contratados por trimestre y departamentos que contrataron más empleados que el promedio.

## Endpoints

### Estado del Servidor

- **GET /ping**: Verifica que el servidor esté funcionando. Devuelve "Pong!".

### Gestión de Empleados

- **POST /employee**: Crea un nuevo empleado.
- **POST /load-hist-employee**: Carga datos históricos de empleados desde un archivo CSV.
- **POST /snapshot-employee**: Crea un snapshot en formato Avro de la tabla de empleados.
- **POST /restore-employees**: Restaura los datos de empleados desde el snapshot Avro más reciente.

### Gestión de Departamentos

- **POST /department**: Crea un nuevo departamento.
- **POST /load-hist-department**: Carga datos históricos de departamentos desde un archivo CSV.
- **POST /snapshot-department**: Crea un snapshot en formato Avro de la tabla de departamentos.
- **POST /restore-departments**: Restaura los datos de departamentos desde el snapshot Avro más reciente.

### Gestión de Trabajos

- **POST /job**: Crea un nuevo trabajo.
- **POST /load-hist-job**: Carga datos históricos de trabajos desde un archivo CSV.
- **POST /snapshot-job**: Crea un snapshot en formato Avro de la tabla de trabajos.
- **POST /restore-jobs**: Restaura los datos de trabajos desde el snapshot Avro más reciente.

### Consultas Avanzadas

- **GET /employees-hired-by-quarter**: Lista la cantidad de empleados contratados por cada trabajo y departamento en 2021, dividido por trimestre.
- **GET /departments-above-average-hires**: Lista los departamentos que contrataron más empleados que el promedio en 2021, ordenados por la cantidad de empleados contratados en orden descendente.

## Requisitos

- Python 3.7+
- Flask
- Flask-RESTful
- SQLAlchemy
- psycopg2
- Marshmallow
- Google Cloud Storage

## Instalación

1. Clonar el repositorio:
    ```bash
    git clone <repositorio>
    cd <directorio-del-repositorio>
    ```

2. Crear un entorno virtual y activarlo:
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```

3. Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

4. Configurar las variables de entorno para Google Cloud Storage y la base de datos PostgreSQL.

5. Ejecutar la aplicación:
    ```bash
    flask run
    ```

## Configuración

Asegúrate de configurar la cadena de conexión de la base de datos PostgreSQL en `app.config['SQLALCHEMY_DATABASE_URI']`. La cadena de conexión debe incluir el nombre de usuario, la contraseña, la dirección del host y el nombre de la base de datos.

## Uso

La API está diseñada para interactuar con datos almacenados en Google Cloud Storage. Asegúrate de tener configurado el acceso a tu bucket de almacenamiento y que los archivos CSV y los snapshots Avro estén correctamente ubicados.

### Ejemplo de Peticiones

- **Cargar datos históricos de empleados**:
    ```bash
    curl -X POST http://localhost:5000/load-hist-employee
    ```

- **Crear un snapshot de empleados**:
    ```bash
    curl -X POST http://localhost:5000/snapshot-employee
    ```

- **Restaurar empleados desde un snapshot**:
    ```bash
    curl -X POST http://localhost:5000/restore-employees
    ```

## Despliegue con Docker

1. Construir la imagen Docker:
    ```bash
    docker build -t api_service .
    ```

2. Ejecutar el contenedor Docker:
    ```bash
    docker run -e PORT=5000 -p 5000:5000 api_service
    ```

## Despliegue en Google Cloud Run

La aplicación está desplegada en Google Cloud Run y está accesible en la siguiente URL:

[https://globant-challenge-22-qmij3rko2a-ue.a.run.app](https://globant-challenge-22-qmij3rko2a-ue.a.run.app/ping)


## Visualización de Datos

Puedes visualizar los datos utilizando Looker Studio en el siguiente enlace:

[https://lookerstudio.google.com/reporting/7b83e710-738b-4544-8801-09191d9acf11](https://lookerstudio.google.com/reporting/7b83e710-738b-4544-8801-09191d9acf11)


docker build -t api_service .

docker run -e PORT=5000 -p 5000:5000 api_service


