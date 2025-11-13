from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.controllers.controller import router
from app.config import settings
import logging
import time

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Fashia Backend API",
    description="Backend API for Fashia application",
    version="1.0.0"
)

# Global request logging middleware - MUST be added FIRST
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Log incoming request
    logger.info("=" * 80)
    logger.info(f"🌐 INCOMING REQUEST")
    logger.info(f"   Method: {request.method}")
    logger.info(f"   URL: {request.url}")
    logger.info(f"   Path: {request.url.path}")
    logger.info(f"   Query Params: {dict(request.query_params)}")
    logger.info(f"   Client IP: {request.client.host if request.client else 'Unknown'}")
    logger.info(f"   User-Agent: {request.headers.get('user-agent', 'Unknown')}")
    logger.info(f"   X-Forwarded-For: {request.headers.get('x-forwarded-for', 'Not present')}")
    logger.info("=" * 80)
    
    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        
        # Log response
        logger.info("=" * 80)
        logger.info(f"✅ REQUEST COMPLETED")
        logger.info(f"   Path: {request.url.path}")
        logger.info(f"   Status Code: {response.status_code}")
        logger.info(f"   Process Time: {process_time:.2f}ms")
        logger.info("=" * 80)
        
        return response
        
    except Exception as e:
        process_time = (time.time() - start_time) * 1000
        logger.error("=" * 80)
        logger.error(f"💥 REQUEST FAILED")
        logger.error(f"   Path: {request.url.path}")
        logger.error(f"   Error: {str(e)}")
        logger.error(f"   Process Time: {process_time:.2f}ms")
        logger.error("=" * 80)
        import traceback
        logger.error(f"📋 Traceback:\n{traceback.format_exc()}")
        raise

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Startup event
@app.on_event("startup")
async def startup_event():
    logger.info("🚀" + "=" * 79)
    logger.info("🚀 FASHIA BACKEND API - STARTING UP")
    logger.info("🚀" + "=" * 79)
    logger.info(f"📝 Title: {app.title}")
    logger.info(f"📝 Version: {app.version}")
    logger.info(f"📝 Environment: {getattr(settings, 'ENVIRONMENT', 'Unknown')}")
    logger.info(f"🌐 Listening on: 0.0.0.0:80")
    logger.info("🚀" + "=" * 79)
    logger.info("✅ Application startup complete - ready to accept requests")
    logger.info("🚀" + "=" * 79)

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑" + "=" * 79)
    logger.info("🛑 FASHIA BACKEND API - SHUTTING DOWN")
    logger.info("🛑" + "=" * 79)

# Include routers
app.include_router(router)

# Log all registered routes on startup
@app.on_event("startup")
async def log_routes():
    logger.info("📍" + "=" * 79)
    logger.info("📍 REGISTERED ROUTES:")
    for route in app.routes:
        if hasattr(route, 'methods'):
            methods = ', '.join(route.methods)
            logger.info(f"   {methods:8} {route.path}")
    logger.info("📍" + "=" * 79)