from fastapi import FastAPI
import uvicorn
from routes import api_endpoints

app = FastAPI()


@app.get("/dfwebapi/test")
def read_root():
    return {'message': 'web api microservice is working.'}


app.include_router(api_endpoints.router, prefix="/dfwebapi")

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8420)   ##### uvicorn main:app --host 0.0.0.0 --port 8420 --reload
