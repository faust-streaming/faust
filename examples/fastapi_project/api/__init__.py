from api.my_api import router as my_api_router
from fastapi import APIRouter

router = APIRouter()

router.include_router(my_api_router)
