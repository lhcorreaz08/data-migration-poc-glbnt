# data-migration-poc-glbnt


docker build -t api_service .

docker run -e PORT=5000 -p 5000:5000 api_service


gcloud run deploy --source .
globant-challenge-10