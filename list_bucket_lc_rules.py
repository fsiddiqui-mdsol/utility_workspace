import boto3
from botocore.exceptions import ClientError
import sys

def list_lifecycle_rule_keys(bucket_name):
    """
    Connects to an S3 bucket and prints the ID and key prefix for each lifecycle rule.

    Args:
        bucket_name (str): The name of the S3 bucket.
    """
    s3_client = boto3.client('s3')

    try:
        # Get the lifecycle configuration for the specified bucket
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)

        # The configuration is a dictionary containing a list of 'Rules'
        rules = response.get('Rules', [])

        if not rules:
            print(f"No lifecycle rules found for bucket '{bucket_name}'.")
            return

        print(f"Found {len(rules)} lifecycle rules for bucket '{bucket_name}':\n")
        
        # Iterate through each rule and extract its ID and key prefix
        for rule in rules:
            rule_id = rule.get('ID')
            
            # The key is specified in the 'Filter' dictionary
            # It's important to handle cases where a filter might not exist
            # or where the rule applies to the entire bucket (no prefix)
            filter_config = rule.get('Filter', {})
            prefix = filter_config.get('Prefix', 'N/A (Applies to all objects)')
            
            print(f"Rule ID: {rule_id}, Key Prefix: {prefix}")

    except ClientError as e:
        # Handle specific AWS-related errors, like "NoSuchLifecycleConfiguration"
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            print(f"No lifecycle configuration exists for bucket '{bucket_name}'.")
        else:
            print(f"An AWS error occurred: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)


if __name__ == '__main__':
    # You must replace this with your actual bucket name
    bucket_name = sys.argv[1]
    list_lifecycle_rule_keys(bucket_name)