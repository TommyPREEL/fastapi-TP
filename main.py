from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from fastapi.responses import StreamingResponse
import io
from dotenv import load_dotenv
import os

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

app = FastAPI()

# Configuration de Boto3
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

# Nom du bucket S3
BUCKET_NAME = os.getenv('BUCKET_NAME')

@app.get("/ping")
async def ping():
    return "pong"

@app.put("/api/file/{filename}")
async def upload_file(filename: str, file: UploadFile = File(...)):
    try:
        # Vérifier si le fichier est bien un objet file-like
        if not file.file:
            raise HTTPException(status_code=400, detail="No file uploaded")
        
        # Téléverser le fichier sur S3
        s3.upload_fileobj(file.file, BUCKET_NAME, filename)
        
        return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credentials not available")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

@app.get("/api/file/{filename}")
async def download_file(filename: str):
    try:
        file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=filename)
        return StreamingResponse(io.BytesIO(file_obj['Body'].read()), media_type='application/octet-stream')
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credentials not available")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail="File not found")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to download file: {e}")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
