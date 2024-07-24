# from fastapi import FastAPI

# app = FastAPI()

# @app.get("/ping")
# async def ping():
#     return "pong"

# @app.get("/pong")
# async def ping():
#     return "ping"

import logging
import uuid
from fastapi import FastAPI, Request, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError, PartialCredentialsError
from fastapi.responses import StreamingResponse
import io
from dotenv import load_dotenv
import os
import datetime
# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

app = FastAPI()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
# Configuration de Boto3
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_REGION'),
                  )

# Nom du bucket S3
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')


# dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

dynamodb = boto3.client('dynamodb',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_REGION'),
                  )
print(os.getenv('AWS_ACCESS_KEY_ID'))
print(os.getenv('AWS_SECRET_ACCESS_KEY'))
print(os.getenv('AWS_S3_BUCKET_NAME'))

@app.get("/ping")
async def ping():
    return "pong"


# @app.put("/api/file/{filename}")
# async def upload_file(filename: str, request: Request):
#     try:
#         # Lire le fichier depuis le corps de la requête
#         file_content = await request.body()

#         # Convertir le contenu en un objet BytesIO
#         file_stream = io.BytesIO(file_content)
        
#         # Téléverser le fichier sur S3
#         s3.upload_fileobj(file_stream, BUCKET_NAME, filename)
        
#         return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
#     except NoCredentialsError:
#         raise HTTPException(status_code=500, detail="Credentials not available")
#     except ClientError as e:
#         raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

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

@app.get("/api/file/{filename}")
async def download_file(request: Request, filename: str):
    try:
        file_obj = io.BytesIO()
        s3.download_fileobj(AWS_S3_BUCKET_NAME, filename, file_obj)
        file_obj.seek(0)

        downloader_ip = request.client.host

        # Log download information in DynamoDB
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
    

@app.delete("/api/file/{filename}")
async def delete_file(request: Request, filename: str):
    try:
        s3.delete_object(Bucket=AWS_S3_BUCKET_NAME, Key=filename)
        dynamodb.put_item(
            TableName="FileUpload",
            Key={'deletion_date': {'S': datetime.datetime.now().isoformat()}}
        )
        return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credentials not available")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
