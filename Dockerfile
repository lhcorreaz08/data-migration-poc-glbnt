# Dockerfile
FROM python:3.9.17-bookworm

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True
# Copy local code to the container image.
ENV APP_HOME /back-end
WORKDIR $APP_HOME
COPY . ./

# Upgrade pip and install dependencies from requirements.txt
RUN pip install --no-cache-dir --upgrade pip 
RUN pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app