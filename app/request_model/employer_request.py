from typing import List, Optional
from pydantic import BaseModel

class EmployerFilterRequest(BaseModel):
    name: Optional[str] = ""
    city: Optional[List[str]] = None
    state: Optional[List[str]] = None
    address: Optional[str] = ""
    zipcode: Optional[str] = ""
    roles: Optional[List[str]] = None
    specialties: Optional[List[str]] = None
    provider_first_name: Optional[str] = ""
    provider_last_name: Optional[str] = ""
    type: Optional[List[str]] = None
    subtype: Optional[List[str]] = None
    facilities: Optional[List[str]] = None
    coords: Optional[List[dict]] = None
    page: int = 1
    per_page: int = 25
    sort_by: str = "name"
    sort_order: str = "ASC"