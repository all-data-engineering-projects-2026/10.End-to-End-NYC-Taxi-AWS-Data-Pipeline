import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

# AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id', fallback='')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key', fallback='')
AWS_REGION = parser.get('aws', 'aws_region', fallback='ap-south-1')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')

# Paths
# OUTPUT_PATH = parser.get('file_paths', 'output_path', fallback='/opt/airflow/data/output')

# Redshift (warehouse layer — Gold -> Redshift, mirrors the tutorial's final stage)
REDSHIFT_HOST = parser.get('redshift', 'host', fallback='')
REDSHIFT_PORT = parser.get('redshift', 'port', fallback='5439')
REDSHIFT_DB = parser.get('redshift', 'dbname', fallback='dev')
REDSHIFT_USER = parser.get('redshift', 'user', fallback='')
REDSHIFT_PASSWORD = parser.get('redshift', 'password', fallback='')
REDSHIFT_IAM_ROLE = parser.get('redshift', 'iam_role', fallback='')
REDSHIFT_SCHEMA = "nyc_taxi"
