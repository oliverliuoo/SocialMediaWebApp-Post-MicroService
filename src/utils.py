import logging
import uuid

import boto3
from botocore.exceptions import ClientError

import config

# config logger
logging.basicConfig(filename=config.log_file, level=logging.INFO)
logger = logging.getLogger(__name__)

s3_config = {
        'region_name': config.aws_region,
        'aws_access_key_id': config.s3_access_key,
        'aws_secret_access_key': config.s3_secrete_key
    }


def create_presigned_put_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to upload an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3', **s3_config)
    try:
        response = s3_client.generate_presigned_url('put_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logger.error(e)
        return None

    # The response contains the presigned URL
    return response


def delete_s3_object(bucket_name, object_name):
    """Generate a presigned URL S3 POST request to upload a file

    :param bucket_name: string
    :param object_name: string
    :return: None if error.
    """

    # Generate a presigned S3 POST URL
    s3_client = boto3.client('s3', **s3_config)
    try:
        response = s3_client.delete_object(
            Bucket=bucket_name,
            Key=object_name
        )
    except ClientError as e:
        logger.error(e)
        return None

    # The response contains the presigned URL and required fields
    return response


if __name__ == '__main__':
    obj_name = str(uuid.uuid4())
    url_get = create_presigned_put_url(config.s3_bucket_name, 'requirement.txt')
    url_put = create_presigned_put_url(config.s3_bucket_name, 'requirement.txt')
    x = delete_s3_object(config.s3_bucket_name, '90be0999-5a29-4548-8fb6-803818050e32')
    print(x)
    print(url_put == url_get)
    print(url_get)
    print(url_put.split('?')[0])
