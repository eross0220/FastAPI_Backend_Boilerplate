from fastapi import APIRouter

from app.api.v1 import products
from app.api.v1 import pipelines
from app.api.v1 import downloads

api_router = APIRouter()
api_router.include_router(products.router, prefix="/products", tags=["products"])
api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(downloads.router, prefix="/downloads", tags=["downloads"])