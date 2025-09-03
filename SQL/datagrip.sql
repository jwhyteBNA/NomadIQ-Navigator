
SET s3_endpoint = 'http://localhost:9000';
SET s3_access_key_id = 'admin';
SET s3_secret_access_key = 'password';
SET s3_url_style = 'path';
SET s3_region = 'us-east-1';
SET s3_use_ssl = false;

DETACH DATABASE catalog;
LOAD ducklake;
ATTACH 'ducklake:/Users/jgwmacbookpro/Workspace/Projects/parks-wayfinder-pipeline/catalog.ducklake'
AS minio_lake (DATA_PATH 's3://nomadiq/');
USE minio_lake;