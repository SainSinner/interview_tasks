from sqlalchemy.orm import Session
from models import Character

def get_all_characters(db: Session):
    return db.query(Character).all()