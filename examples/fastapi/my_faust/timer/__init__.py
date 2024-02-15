from fastapi import APIRouter
from my_faust.timer.my_timer import router as my_timer_router

router = APIRouter()

router.include_router(my_timer_router)
