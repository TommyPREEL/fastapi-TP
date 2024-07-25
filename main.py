import uuid
import os
import datetime
import io
from fastapi import FastAPI, Request, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError, PartialCredentialsError
from dotenv import load_dotenv


# Create fast API instance
app = FastAPI()

# Load environment variables
load_dotenv()

# Set environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

# Boto3 configuration for S3
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION'),
)

# Boto3 configuration for dynamodb
dynamodb = boto3.client('dynamodb',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION'),
)


# This endpoint is used for testing.
# It just responding 'pong'
@app.get("/ping")
async def ping():
    return "pong"


# This endpoint allows to upload a file
# @input (UploadFile) file - The file to upload
# @return None
@app.post("/api/file")
async def upload_file(file: UploadFile = File(...)):
    try:
        s3.upload_fileobj(file.file, AWS_S3_BUCKET_NAME, file.filename)
        dynamodb.put_item(
            TableName="FileUpload",
            Item={
                'id': {'S': str(uuid.uuid4())},
                'filename': {'S': file.filename},
                'size': {'N': str(file.size)},
                'upload_date': {'S': datetime.datetime.now().isoformat()},
                'deletion_date': {'S': ""},
            }
        )
        return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credentials not available")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")


# This endpoint allows to download a file
# @input [PathParameters] (str) filename - The file to download
# @return None
@app.get("/api/file/{filename}")
async def download_file(request: Request, filename: str):
    try:
        # Get the file from the S3
        file_obj = io.BytesIO()
        s3.download_fileobj(AWS_S3_BUCKET_NAME, filename, file_obj)
        file_obj.seek(0)

        # Get the client IP
        downloader_ip = request.client.host

        # Insert file info in dynamoDB
        dynamodb.put_item(
            TableName="FileDownload",
            Item={
                'id': {'S': str(uuid.uuid4())},
                'filename': {'S': filename},
                'download_date': {'S': datetime.datetime.now().isoformat()},
                'downloader_ip': {'S': downloader_ip}
            }
        )

        return StreamingResponse(file_obj, media_type="application/octet-stream", headers={"Content-Disposition": f"attachment; filename={filename}"})
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found")
    except NoCredentialsError:
        raise HTTPException(status_code=400, detail="AWS credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete AWS credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# This endpoint allows to delete a file
# @input [PathParameters] (str) file_id - The file id to delete
# @return None
@app.delete("/api/file/{file_id}")
async def delete_file(request: Request, file_id: str):
    try:
        # Retrieve the item from DynamoDB to get the filename
        response = dynamodb.get_item(
            TableName="FileUpload",
            Key={'id': {'S': file_id}}
        )

        # Check if the file exists in DynamoDB
        if 'Item' not in response:
            raise HTTPException(status_code=404, detail="File not found in DynamoDB")

        # Extract the filename from the DynamoDB response
        item = response['Item']
        filename = item.get('filename', {}).get('S')
        
        if not filename:
            raise HTTPException(status_code=404, detail="Filename not found in DynamoDB")

        # Delete the file from S3
        s3.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=filename)

        # Record the deletion date in DynamoDB
        dynamodb.update_item(
            TableName="FileUpload",
            Key={'id': {'S': file_id}},
            UpdateExpression="SET deletion_date = :deletion_date",
            ExpressionAttributeValues={
                ':deletion_date': {'S': str(datetime.datetime.now().isoformat())}
            }
        )

        return JSONResponse(content={"message": "File deleted successfully"}, status_code=200)
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found in S3")
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not available")
    except ClientError as e:
        error_message = e.response['Error'].get('Message', 'An error occurred')
        raise HTTPException(status_code=500, detail=f"S3 error: {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# This endpoint allows to see all the files available in S3
# @input None
# @return None
@app.get("/api/files")
async def get_files(request: Request):
    try:
        # Scan DynamoDB table to retrieve all files
        response = dynamodb.scan(
            TableName="FileUpload",
            FilterExpression="attribute_not_exists(deletion_date) OR deletion_date = :empty",
            ExpressionAttributeValues={
                ':empty': {'S': ''}
            }
        )
        
        # Extract file details from the response
        files = []
        for item in response.get('Items', []):
            file_info = {
                'id': item.get('id', {}).get('S'),
                'filename': item.get('filename', {}).get('S'),
                'size': item.get('size', {}).get('N'),
                'upload_date': item.get('upload_date', {}).get('S')
            }
            files.append(file_info)

        return JSONResponse(content={"files": files}, status_code=200)
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="AWS credentials not available")
    except ClientError as e:
        error_message = e.response['Error'].get('Message', 'An error occurred')
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
