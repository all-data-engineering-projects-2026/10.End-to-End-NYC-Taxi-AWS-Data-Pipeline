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
