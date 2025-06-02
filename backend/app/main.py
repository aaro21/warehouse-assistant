from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.services.lineage.routes import router as lineage_router

app = FastAPI()

# Allow frontend to call backend locally
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register the lineage API routes
app.include_router(lineage_router, prefix="/lineage")