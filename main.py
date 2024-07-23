# from fastapi import FastAPI

# app = FastAPI()

# @app.get("/ping")
# async def ping():
#     return "pong"

# @app.get("/pong")
# async def ping():
#     return "ping"


from fastapi import FastAPI, Request, File, UploadFile, HTTPException
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
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

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

@app.post("/api/file/{filename}")
async def upload_file(filename: str, file: UploadFile = File(...)):
    try:
        # Téléverser le fichier sur S3
        s3.upload_fileobj(file.file, AWS_S3_BUCKET_NAME, filename)
        return JSONResponse(content={"message": "File uploaded successfully"}, status_code=200)
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Credentials not available")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

@app.get("/api/file/{filename}")
async def download_file(filename: str):
    print(f"AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID')}")
    print(f"AWS_SECRET_ACCESS_KEY: {os.getenv('AWS_SECRET_ACCESS_KEY')}")
    print(f"AWS_S3_BUCKET_NAME: {os.getenv('AWS_S3_BUCKET_NAME')}")
    try:
        file_obj = s3.get_object(Bucket=AWS_S3_BUCKET_NAME, Key=filename)
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
