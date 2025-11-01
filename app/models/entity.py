from sqlalchemy import Column, Integer, String, Boolean, Float # type: ignore
from app.db.base import Base # type: ignore

class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, index=True)  # Add an ID column for primary key
    name = Column(String, nullable=False)
    ccn = Column(String)
    npi = Column(Integer)
    ccn_or_npi = Column(String)
    type = Column(String, nullable=False)
    subtype = Column(String)
    nucc_code = Column(String)
    unique_facility_at_location = Column(Boolean, default=True)
    employer_group_type = Column(String, default='none')
    facility_group_type = Column(String, default='none')
    employer_num = Column(Integer, default=0)
    clinical_location_from_provider = Column(Boolean, default=False)
    address = Column(String)
    city = Column(String)
    state_id = Column(Integer)
    zip_code = Column(String)
    address_hash = Column(String)
    is_provider_entity = Column(Boolean, default=False)
    latitude = Column(Float)
    longitude = Column(Float)