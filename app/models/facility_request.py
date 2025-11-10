# entity_request.py
from typing import List, Optional
from pydantic import BaseModel, Field

class FacilityFilterRequest(BaseModel):
    name: Optional[str] = Field(default="", description="Filter by entity name")
    address: Optional[str] = Field(default="", description="Filter by entity address")
    zipcode: Optional[str] = Field(default="", description="Filter by entity zipcode")
    city: Optional[List[str]] = Field(default=[], description="Filter by city")  # Fixed: default=[]
    state: Optional[List[str]] = Field(default=[], description="Filter by state")  # Fixed: default=[]
    type: Optional[List[str]] = Field(default=[], description="Filter by type")  # Fixed: default=[]
    subtype: Optional[List[str]] = Field(default=[], description="Filter by subtype")  # Fixed: default=[]
    roles: Optional[List[str]] = Field(default=[], description="Filter by roles")
    specialties: Optional[List[str]] = Field(default=[], description="Filter by specialties")
    employers: Optional[List[str]] = Field(default=[], description="Filter by employers")
    page: int = Field(default=1, ge=1, description="Page number")
    per_page: int = Field(default=25, ge=1, le=200, description="Items per page")
    sort_by: str = Field(default="name", description="Sort by")
    sort_order: str = Field(default="asc", description="Sort order")
    provider_first_name: Optional[str] = Field(default="", description="Filter by provider first name")  # Fixed: Added Field()
    provider_last_name: Optional[str] = Field(default="", description="Filter by provider last name")  # Fixed: Added Field()
    coords: Optional[List[dict]] = Field(default=None, description="Bounding box coordinates")  # Fixed: Added Field()