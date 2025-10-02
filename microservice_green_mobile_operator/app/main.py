from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from db import get_db
from repository import get_all_characters

app = FastAPI(title="microservice_green_mobile_operator")

# root answer
@app.get("/")
def root():
    return {"message": "отправь запрос"}

# тестовый эндпоинт
@app.get("/test")
def def_test():
    return {"message": "worf work work work!"}

# получить персонажей из bd
@app.get("/bd/characters")
def get_characters(db: Session = Depends(get_db)):
    return get_all_characters(db)