# entity_model.py
from pydantic import BaseModel, Field
from typing import List, Optional

class FacilityEmployer(BaseModel):
    name: str
    ccn_or_npi: str

class FacilityResponse(BaseModel):
    id: Optional[int] = 0
    name: Optional[str] = None
    type: Optional[str] = None
    subtype: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    state_code: Optional[str] = None
    zip_code: Optional[str] = None
    ccn_or_npi: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    employers: List[FacilityEmployer] = []
    roles: List[str] = []
    specialties: List[str] = []
    providers_count: int = 0
    # Removed provider_names field

class PaginatedEntityResponse(BaseModel):
    data: List[FacilityResponse]
    page: int
    per_page: int
    total: int
    total_pages: int