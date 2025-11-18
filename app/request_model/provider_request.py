from typing import List, Optional
from pydantic import BaseModel, Field

class ProviderFilterRequest(BaseModel):
    first_name: Optional[str] = Field(default="", description="Filter by provider first name")
    last_name: Optional[str] = Field(default="", description="Filter by provider last name")
    roles: Optional[List[str]] = Field(default=[], description="Filter by roles")
    specialties: Optional[List[str]] = Field(default=[], description="Filter by specialties")
    facility_cities: Optional[List[str]] = Field(default=[], description="Filter by facility cities")
    facility_states: Optional[List[str]] = Field(default=[], description="Filter by facility states")
    facility_address: Optional[str] = Field(default="", description="Filter by facility address")
    facility_zipcode: Optional[str] = Field(default="", description="Filter by facility zipcode")
    license_state_id: Optional[int] = Field(default=None, description="Filter by license state ID")
    facility_names: Optional[List[str]] = Field(default=[], description="Filter by facility names")
    employer_names: Optional[List[str]] = Field(default=[], description="Filter by employer names")
    facility_types: Optional[List[str]] = Field(default=[], description="Filter by facility types")
    facility_subtypes: Optional[List[str]] = Field(default=[], description="Filter by facility subtypes")
    page: int = Field(default=1, ge=1, description="Page number")
    per_page: int = Field(default=25, ge=1, le=200, description="Items per page")
    sort_by: str = Field(default="name", description="Sort by field")
    sort_order: str = Field(default="asc", description="Sort order (asc/desc)")