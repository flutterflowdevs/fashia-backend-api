from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class StateInfo(BaseModel):
    state_name: str
    state_code: str

class FacilityTypeInfo(BaseModel):
    type: str
    subtype: str = ""  # Default to empty string if null

class FacilityInfo(BaseModel):
    id: int
    name: str
    type: str
    subtype: str = ""  # Default to empty string if null
    city: str
    state_name: str
    state_code: str
    address: str
    zip_code: str
    latitude: float
    longitude: float
    ccn_or_npi: str
    provider_count: int

class EmployerResponse(BaseModel):
    id: int
    name: str
    ccn_or_npi: str
    roles: List[str] = []
    specialties: List[str] = []
    providers_count: int = 0
    facilities: List[FacilityInfo] = []
    facility_cities: List[str] = []
    facility_states: List[StateInfo] = []
    facility_types: List[FacilityTypeInfo] = []

class PaginatedEmployerResponse(BaseModel):
    data: List[EmployerResponse]
    page: int
    per_page: int
    total: int
    total_pages: int