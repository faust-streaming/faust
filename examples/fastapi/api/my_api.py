from typing import Union
from fastapi import APIRouter

from my_faust.table.my_table import greetings_table

router = APIRouter()


@router.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@router.get("/table")
def read_table():
    return [{k: v} for k, v in greetings_table.items()]
