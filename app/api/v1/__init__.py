from fastapi import APIRouter

from app.api.v1 import products
from app.api.v1 import pipelines

api_router = APIRouter()
api_router.include_router(products.router, prefix="/products", tags=["products"])
api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
