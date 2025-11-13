from fastapi import APIRouter
from app.services.get_facilities_data import get_facilities_data
from app.controllers.helper_functions import get_container_environment_info, get_container_user_info
from app.models.facility_request import FacilityFilterRequest
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/")
async def root():
    return {"status": "ok", "message": "Fashia Backend API"}

@router.get("/api/hello")
async def hello():
    return {"message": "Hello from Fashia API!"}

@router.get("/api/health")
async def health():
    return {"status": "ok", "service": "Fashia Backend API"}

@router.get("/health")
async def health():
    return {"status": "ok", "service": "Fashia Backend API"}

@router.post("/entities")
async def get_entities(request: FacilityFilterRequest):
    """POST endpoint to filter and retrieve entities"""
    # Debug: Print the received request data
    logger.info(f"📨 Received request: {request.dict()}")
    
    try:
        result = await get_facilities_data(
            name=request.name,
            cities=request.city,
            states=request.state,
            address=request.address,
            zipcode=request.zipcode,
            types=request.type,
            subtypes=request.subtype,
            roles=request.roles,
            specialties=request.specialties,
            employers=request.employers,
            page=request.page,
            per_page=request.per_page,
            sort_by=request.sort_by,
            sort_order=request.sort_order,
            provider_first_name=request.provider_first_name,
            provider_last_name=request.provider_last_name,
            coords=request.coords
        )
        logger.info(f"✅ Request processed successfully, returning {len(result.data)} entities")
        return result
    except Exception as e:
        logger.error(f"💥 Error in controller: {str(e)}")
        logger.error(f"🔧 Error type: {type(e)}")
        raise

@router.get("/api/db-test")
async def db_test():
    """Test database connection and permissions with detailed container info"""
    logger.info("🧪 Database test endpoint called")
    
    try:
        # Get detailed container info
        user_info = get_container_user_info()
        env_info = get_container_environment_info()
        
        logger.info(f"👤 Container User Info: {user_info}")
        logger.info(f"🏠 Container Environment: {env_info}")
        
        # Log file system permissions for database path
        from app.db.session import DATABASE_PATH
        db_path = DATABASE_PATH
        
        fs_info = {}
        try:
            if os.path.exists(db_path):
                stat_info = os.stat(db_path)
                fs_info['database_file_exists'] = True
                fs_info['database_file_permissions'] = oct(stat_info.st_mode)[-3:]
                fs_info['database_file_owner'] = stat_info.st_uid
                fs_info['database_file_group'] = stat_info.st_gid
                fs_info['database_file_readable'] = os.access(db_path, os.R_OK)
            else:
                fs_info['database_file_exists'] = False
                
            # Check directory permissions
            db_dir = os.path.dirname(db_path)
            if db_dir and os.path.exists(db_dir):
                fs_info['database_dir_readable'] = os.access(db_dir, os.R_OK)
                fs_info['database_dir_executable'] = os.access(db_dir, os.X_OK)
            else:
                fs_info['database_dir_exists'] = False
                
        except Exception as e:
            fs_info['error'] = f"Failed to check filesystem: {e}"
        
        logger.info(f"📁 File System Info: {fs_info}")
        
        # Test database connection
        from app.db.session import get_db_connection
        
        logger.info("🔄 Testing database connection...")
        
        async with get_db_connection() as conn:
            logger.info("✅ Database connection successful")
            
            # Test basic query
            cursor = await conn.execute("SELECT sqlite_version()")
            version = await cursor.fetchone()
            logger.info(f"📊 SQLite version: {version[0]}")
            
            # Test table access
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 5")
            tables = await cursor.fetchall()
            table_names = [table[0] for table in tables]
            logger.info(f"📋 First 5 tables: {table_names}")
            
            # Test entities_enriched table
            cursor = await conn.execute("SELECT COUNT(*) FROM entities_enriched WHERE is_employer = 0")
            facility_count = await cursor.fetchone()
            logger.info(f"🏥 Facilities count: {facility_count[0]}")
            
            return {
                "status": "success",
                "database_connection": "ok",
                "sqlite_version": version[0],
                "sample_tables": table_names,
                "facilities_count": facility_count[0],
                "user_info": user_info,
                "environment_info": env_info,
                "filesystem_info": fs_info
            }
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        # Get info even when there's an error
        user_info = get_container_user_info()
        env_info = get_container_environment_info()
        
        logger.error(f"💥 Database test failed: {error_details}")
        logger.error(f"👤 User info during error: {user_info}")
        logger.error(f"🏠 Environment during error: {env_info}")
        
        return {
            "status": "error",
            "database_connection": "failed",
            "error": str(e),
            "user_info": user_info,
            "environment_info": env_info
        }    