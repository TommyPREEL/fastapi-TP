from fastapi import FastAPI

app = FastAPI()

@app.get("/ping")
async def ping():
    return "pong"

@app.get("/pong")
async def ping():
    return "ping"
