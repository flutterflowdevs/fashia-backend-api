from typing import List, Optional
from pydantic import BaseModel, Field


# Response Model
class ProviderResponse(BaseModel):
    id: int
    npi: int
    first_name: str
    last_name: str
    credentials: Optional[str] = None
    roles: List[str] = []
    specialties: List[str] = []
    facility_cities: List[str] = []
    facility_states: List[str] = []
    licensure_states: List[str] = []
    facility_names: List[dict] = []  # [{"name": "...", "ccn_or_npi": "...", "type": "...", "subtype": "...", "address": "...", "zip_code": "...", "latitude": ..., "longitude": ..., "state_name": "...", "state_code": "...", "city": "...", "provider_count": ...}]
    employer_names: List[dict] = []  # [{"name": "...", "ccn_or_npi": "..."}]
    facility_types: List[str] = []
    facility_subtypes: List[str] = []

# Reuse PaginatedEntityResponse or create PaginatedProviderResponse
class PaginatedProviderResponse(BaseModel):
    data: List[ProviderResponse]
    page: int
    per_page: int
    total: int
    total_pages: int
