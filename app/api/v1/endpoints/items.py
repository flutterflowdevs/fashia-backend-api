from fastapi import APIRouter, Depends
from app.schemas.item import ItemCreate, ItemUpdate, Item
from app.crud.item import create_item, get_item, update_item, delete_item
from app.db.session import get_db
from sqlalchemy.orm import Session

router = APIRouter()

@router.post("/", response_model=Item)
def create_item_endpoint(item: ItemCreate, db: Session = Depends(get_db)):
    return create_item(db=db, item=item)

@router.get("/{item_id}", response_model=Item)
def read_item(item_id: int, db: Session = Depends(get_db)):
    return get_item(db=db, item_id=item_id)

@router.put("/{item_id}", response_model=Item)
def update_item_endpoint(item_id: int, item: ItemUpdate, db: Session = Depends(get_db)):
    return update_item(db=db, item_id=item_id, item=item)

@router.delete("/{item_id}", response_model=Item)
def delete_item_endpoint(item_id: int, db: Session = Depends(get_db)):
    return delete_item(db=db, item_id=item_id)