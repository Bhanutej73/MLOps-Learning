## MLFLOW on AWS

sudo apt update

sudo apt install python3-pip

sudo apt install pipenv

sudo apt install virtualenv

mkdir mlflow

cd mlflow

pipenv install mlflow

pipenv install awscli

pipenv install boto3

pipenv shell

## Then set aws credentials
aws configure

#Finally
mlflow server -h 0.0.0.0 --default-artifact-root s3://mltracking --allowed-hosts "*"