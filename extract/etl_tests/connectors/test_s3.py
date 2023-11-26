import pytest
from pathlib import Path
import yaml
import boto3

# set up a pytest namespace
def pytest_namespace():
    return {
        's3_bucket_name': None
    }

@pytest.fixture
def setup():
    
    # retrieve config values from yaml file
    yaml_file_path = __file__.replace(".py", ".yaml")

    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file.")

    s3_bucket_name = yaml_config.get("s3_bucket_name")

    # store the config values in the pytest namespace
    pytest.s3_bucket_name = s3_bucket_name

def test_bucket_exists(setup):
    
    # retrieve the bucket name from the pytest namespace
    s3_bucket_name = pytest.s3_bucket_name

    s3 = boto3.client('s3')

    # retrieve the s3 bucket 
    response = s3.head_bucket(Bucket=s3_bucket_name)

    assert response is not None
    assert response.get("ResponseMetadata") is not None
    assert response.get("ResponseMetadata").get("HTTPStatusCode") == 200
