from fastapi import APIRouter

from api.my_api import router as my_api_router

router = APIRouter()

router.include_router(my_api_router)
