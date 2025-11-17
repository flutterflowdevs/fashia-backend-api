from fastapi import APIRouter
from app.services.get_facilities_data import get_facilities_data
from app.services.get_employer_data import get_employer_data
from app.request_model.facility_request import FacilityFilterRequest
from app.request_model.employer_request import EmployerFilterRequest

router = APIRouter()

@router.get("/api/hello")
async def hello():
    return {"message": "Hello from Fashia API!"}

@router.get("/api/health")
async def health():
    return {"status": "ok", "service": "Fashia Backend API"}

@router.post("/entities")
async def get_entities(request: FacilityFilterRequest):
    """POST endpoint to filter and retrieve entities"""
    # Debug: Print the received request data
    print(f"Received request: {request.dict()}")
    
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
        return result
    except Exception as e:
        print(f"Error in controller: {str(e)}")
        print(f"Error type: {type(e)}")
        raise

@router.post("/get_employers")
async def get_employers(request: EmployerFilterRequest):
    """POST endpoint to filter and retrieve employers"""
    # Debug: Print the received request data
    print(f"Received employer request: {request.dict()}")
    
    try:
        result = await get_employer_data(
            name=request.name,
            cities=request.city,
            states=request.state,
            address=request.address,
            zipcode=request.zipcode,
            types=request.type,
            subtypes=request.subtype,
            facilities=request.facilities,
            roles=request.roles,
            specialties=request.specialties,
            page=request.page,
            per_page=request.per_page,
            sort_by=request.sort_by,
            sort_order=request.sort_order,
            provider_first_name=request.provider_first_name,
            provider_last_name=request.provider_last_name,
            coords=request.coords
        )
        return result
    except Exception as e:
        print(f"Error in employer controller: {str(e)}")
        print(f"Error type: {type(e)}")
        raise    