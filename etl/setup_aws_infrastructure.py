import boto3
from datetime import datetime

def create_s3_bucket():
    """Create S3 bucket for weather data"""
    s3 = boto3.client('s3')
    bucket_name = f"weather-data-lake-bradley-{datetime.now().strftime('%Y%m%d')}"
    
    try:
        s3.create_bucket(Bucket=bucket_name)
        
        folders = ['raw/weather/', 'processed/weather/', 'scripts/glue/']
        for folder in folders:
            s3.put_object(Bucket=bucket_name, Key=folder)
        
        with open('.aws_config', 'w') as f:
            f.write(f"S3_BUCKET={bucket_name}\n")
            f.write(f"AWS_REGION=us-east-1\n")
        
        print(f"Created bucket: {bucket_name}")
        return bucket_name
        
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket name taken, try again")
        return None
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket already exists")
        return bucket_name
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    create_s3_bucket()
    