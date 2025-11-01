from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.controllers.controller import router
from app.config import settings

app = FastAPI(
    title="Fashia Backend API",
    description="Backend API for Fashia application",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router)
