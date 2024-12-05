import boto3
import re

# Initialize S3 client
s3 = boto3.client('s3')

# Sanitize book tags function
def sanitize_tag_value(value):
    """Ensure tag value complies with tag restrictions by removing restricted characters and truncating values over 256 characters."""
    sanitized_value = re.sub(r'[^a-zA-Z0-9 _\.\:/=\+\-@]', '', value)
    return sanitized_value[:256] 

def lambda_handler(event, context):
    try:
        # Extract bucket name and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Retrieve the uploaded file
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Extract title and author
        title_match = re.search(r'^Title:\s*(.+)', file_content, re.MULTILINE)
        author_match = re.search(r'^Author:\s*(.+)', file_content, re.MULTILINE)
        
        title = title_match.group(1) if title_match else "Unknown"
        author = author_match.group(1) if author_match else "Unknown"
        
        # Sanitize the extracted values
        title = sanitize_tag_value(title)
        author = sanitize_tag_value(author)
        
        # Apply tags to S3 object
        tags = {
            'TagSet': [
                {'Key': 'Title', 'Value': title},
                {'Key': 'Author', 'Value': author}
            ]
        }
        s3.put_object_tagging(Bucket=bucket_name, Key=object_key, Tagging=tags)
        
        print(f"Tags successfully added to {object_key}: Title={title}, Author={author}")
        
    except Exception as e:
        print(f"Error processing object {object_key} from bucket {bucket_name}: {str(e)}")
        raise
