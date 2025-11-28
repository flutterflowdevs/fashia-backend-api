import os
import logging
from typing import List, Optional
from fastapi import HTTPException
from app.db.session import get_db_connection
from app.models.employer_model import EmployerResponse, PaginatedEmployerResponse
import asyncio
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def to_title_case(text: str) -> str:
    """Convert text to title case"""
    if not text:
        return text
    return text.title()

async def execute_subquery(conn, query: str, params: List, field_name: str):
    """Execute a subquery and return the result with field name"""
    try:
        logger.debug(f"Executing subquery for {field_name}: {query[:100]}... with params: {params}")
        cursor = await conn.execute(query, params)
        result = await cursor.fetchone()
        logger.debug(f"Subquery {field_name} result: {result}")
        return field_name, result[0] if result else None
    except Exception as e:
        logger.error(f"Error in subquery for {field_name}: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        return field_name, None

async def get_employer_data(
    name: Optional[str] = "",
    cities: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    address: Optional[str] = "",
    zipcode: Optional[str] = "",
    roles: Optional[List[str]] = None,
    specialties: Optional[List[str]] = None,
    provider_first_name: Optional[str] = "",
    provider_last_name: Optional[str] = "",
    types: Optional[List[str]] = None,
    subtypes: Optional[List[str]] = None,
    facilities: Optional[List[str]] = None,
    coords: Optional[List[dict]] = None,
    page: int = 1,
    per_page: int = 25,
    sort_by: str = "name",
    sort_order: str = "ASC",
):
    """
    Fetch paginated employers from local SQLite database.
    Employers are entities where is_employer = 1.
    """
    # Log function entry and parameters
    logger.info("🔍 get_employer_data called")
    logger.info(f"📊 Parameters - name: {name}, cities: {cities}, states: {states}, page: {page}, per_page: {per_page}")
    
    # Handle None values for lists
    cities = cities or []
    states = states or []
    roles = roles or []
    specialties = specialties or []
    types = types or []
    subtypes = subtypes or []
    facilities = facilities or []
    coords = coords or []
    
    # Validate sort parameters
    valid_sort_fields = ["name", "role", "specialty", "providers_count", "city", "state", "type", "subtype", "facility"]
    if sort_by not in valid_sort_fields:
        logger.warning(f"Invalid sort_by: {sort_by}, defaulting to 'name'")
        sort_by = "name"
    
    sort_order = sort_order.upper()
    if sort_order not in ["ASC", "DESC"]:
        logger.warning(f"Invalid sort_order: {sort_order}, defaulting to 'ASC'")
        sort_order = "ASC"
    
    logger.info(f"🎯 Final sort - by: {sort_by}, order: {sort_order}")
    
    # Check if location filters are provided
    has_location_filters = bool(cities or states or address or zipcode or types or subtypes or facilities or coords)
    
    try:
        async with get_db_connection() as conn:
            logger.info("✅ Database connection established successfully")
            
            # Set query optimizations
            await conn.execute("PRAGMA temp_store = MEMORY")
            await conn.execute("PRAGMA cache_size = -64000")
            
            # Build base filters for employers
            entity_params = []
            filters = ["e.is_employer = 1"]
            logger.info("Building query filters...")

            # Employer name contains search
            if name:
                filters.append("LOWER(e.name) LIKE ?")
                entity_params.append(f"%{name.lower()}%")
                logger.debug(f"Added employer name filter: {name}")

            # Build provider filter conditions
            provider_conditions = []
            provider_params = []
            
            if provider_first_name and provider_last_name:
                provider_conditions.append("(LOWER(pe.first_name) = LOWER(?) AND LOWER(pe.last_name) = LOWER(?))")
                provider_params.extend([provider_first_name, provider_last_name])
                logger.debug(f"Added provider name filter: {provider_first_name} {provider_last_name}")
            elif provider_first_name:
                provider_conditions.append("LOWER(pe.first_name) = LOWER(?)")
                provider_params.append(provider_first_name)
                logger.debug(f"Added provider first name filter: {provider_first_name}")
            elif provider_last_name:
                provider_conditions.append("LOWER(pe.last_name) = LOWER(?)")
                provider_params.append(provider_last_name)
                logger.debug(f"Added provider last name filter: {provider_last_name}")
            
            # Build COMBINED filter for roles and specialties
            combined_conditions = []
            combined_params = []
            
            # Add provider name conditions
            if provider_conditions:
                combined_conditions.extend(provider_conditions)
                combined_params.extend(provider_params)
            
            # Add role conditions
            if roles:
                for role in roles:
                    combined_conditions.append("LOWER(rsc.role) = LOWER(?)")
                    combined_params.append(role)
                logger.debug(f"Added role filters: {roles}")
            
            # Add specialty conditions
            if specialties:
                for specialty in specialties:
                    combined_conditions.append("LOWER(TRIM(rsc.specialty)) = LOWER(?)")
                    combined_params.append(specialty)
                logger.debug(f"Added specialty filters: {specialties}")
            
            # Combined WHERE clause for roles and specialties
            combined_where_clause = " AND ".join(combined_conditions) if combined_conditions else "1=1"
            
            # Combine all provider-related conditions for the main filter - FIXED LOGIC
            if combined_conditions:
                combined_where = " AND ".join(combined_conditions)
                
                # Build facility filters for provider validation
                provider_facility_filters = []
                provider_facility_params = []
                
                if cities:
                    city_conditions = []
                    for city in cities:
                        city_conditions.append("LOWER(provider_fac.city) = ?")
                        provider_facility_params.append(city.lower())
                    provider_facility_filters.append(f"({' OR '.join(city_conditions)})")
                
                if states:
                    state_conditions = []
                    for state in states:
                        state_conditions.append("LOWER(provider_fac_state.state_name) = ?")
                        provider_facility_params.append(state.lower())
                    provider_facility_filters.append(f"({' OR '.join(state_conditions)})")
                
                if address:
                    provider_facility_filters.append("LOWER(provider_fac.address) LIKE ?")
                    provider_facility_params.append(f"%{address.lower()}%")
                
                if zipcode:
                    provider_facility_filters.append("provider_fac.zip_code = ?")
                    provider_facility_params.append(zipcode)
                
                if types:
                    type_conditions = []
                    for facility_type in types:
                        type_conditions.append("LOWER(provider_fac.type) = ?")
                        provider_facility_params.append(facility_type.lower())
                    provider_facility_filters.append(f"({' OR '.join(type_conditions)})")
                
                if subtypes:
                    subtype_conditions = []
                    for subtype in subtypes:
                        subtype_conditions.append("LOWER(provider_fac.subtype) = ?")
                        provider_facility_params.append(subtype.lower())
                    provider_facility_filters.append(f"({' OR '.join(subtype_conditions)})")
                
                if facilities:
                    facility_conditions = []
                    for facility_name in facilities:
                        facility_conditions.append("LOWER(provider_fac.name) = ?")
                        provider_facility_params.append(facility_name.lower())
                    provider_facility_filters.append(f"({' OR '.join(facility_conditions)})")
                
                # Coordinate filter
                if coords and len(coords) >= 2:
                    lats = [coord.get('lat') for coord in coords if coord.get('lat') is not None]
                    lngs = [coord.get('lng') for coord in coords if coord.get('lng') is not None]
                    
                    if lats and lngs:
                        lat_min, lat_max = min(lats), max(lats)
                        lng_min, lng_max = min(lngs), max(lngs)
                        
                        provider_facility_filters.append("provider_fac.latitude BETWEEN ? AND ?")
                        provider_facility_params.extend([lat_min, lat_max])
                        provider_facility_filters.append("provider_fac.longitude BETWEEN ? AND ?")
                        provider_facility_params.extend([lng_min, lng_max])
                
                provider_facility_where = " AND ".join(provider_facility_filters) if provider_facility_filters else "1=1"
                
                filters.append(f"""EXISTS (
                    SELECT 1 FROM provider_facility_employer_linked pfel
                    INNER JOIN provider_employer pe ON pe.provider_id = pfel.provider_id
                    INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    INNER JOIN entities_enriched provider_fac ON provider_fac.ccn_or_npi = pfel.facility_npi_or_ccn
                    LEFT JOIN states provider_fac_state ON provider_fac_state.state_id = provider_fac.state_id
                    WHERE pfel.employer_npi_or_ccn = e.ccn_or_npi
                    AND ({combined_where})
                    AND ({provider_facility_where})
                )""")
                entity_params.extend(combined_params + provider_facility_params)

            # CRITICAL: Add location-based employer filtering
            # When location filters are provided, only show employers that have facilities in those locations
            if has_location_filters:
                location_filters = []
                location_params = []
                
                if cities:
                    city_conditions = []
                    for city in cities:
                        city_conditions.append("LOWER(loc_fac.city) = ?")
                        location_params.append(city.lower())
                    location_filters.append(f"({' OR '.join(city_conditions)})")
                
                if states:
                    state_conditions = []
                    for state in states:
                        state_conditions.append("LOWER(loc_state.state_name) = ?")
                        location_params.append(state.lower())
                    location_filters.append(f"({' OR '.join(state_conditions)})")
                
                if address:
                    location_filters.append("LOWER(loc_fac.address) LIKE ?")
                    location_params.append(f"%{address.lower()}%")
                
                if zipcode:
                    location_filters.append("loc_fac.zip_code = ?")
                    location_params.append(zipcode)
                
                if types:
                    type_conditions = []
                    for facility_type in types:
                        type_conditions.append("LOWER(loc_fac.type) = ?")
                        location_params.append(facility_type.lower())
                    location_filters.append(f"({' OR '.join(type_conditions)})")
                
                if subtypes:
                    subtype_conditions = []
                    for subtype in subtypes:
                        subtype_conditions.append("LOWER(loc_fac.subtype) = ?")
                        location_params.append(subtype.lower())
                    location_filters.append(f"({' OR '.join(subtype_conditions)})")
                
                # ADDED: Facility name filter
                if facilities:
                    facility_conditions = []
                    for facility_name in facilities:
                        facility_conditions.append("LOWER(loc_fac.name) = ?")
                        location_params.append(facility_name.lower())
                    location_filters.append(f"({' OR '.join(facility_conditions)})")
                
                # Coordinate filter
                if coords and len(coords) >= 2:
                    lats = [coord.get('lat') for coord in coords if coord.get('lat') is not None]
                    lngs = [coord.get('lng') for coord in coords if coord.get('lng') is not None]
                    
                    if lats and lngs:
                        lat_min, lat_max = min(lats), max(lats)
                        lng_min, lng_max = min(lngs), max(lngs)
                        
                        location_filters.append("loc_fac.latitude BETWEEN ? AND ?")
                        location_params.extend([lat_min, lat_max])
                        location_filters.append("loc_fac.longitude BETWEEN ? AND ?")
                        location_params.extend([lng_min, lng_max])
                
                location_where = " AND ".join(location_filters)
                
                filters.append(f"""EXISTS (
                    SELECT 1 FROM provider_facility_employer_linked pfel_loc
                    INNER JOIN entities_enriched loc_fac ON loc_fac.ccn_or_npi = pfel_loc.facility_npi_or_ccn
                    LEFT JOIN states loc_state ON loc_state.state_id = loc_fac.state_id
                    WHERE pfel_loc.employer_npi_or_ccn = e.ccn_or_npi
                    AND ({location_where})
                )""")
                entity_params.extend(location_params)
                
                logger.info(f"📍 Added location filter - employers must have facilities matching location criteria")

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            logger.info(f"📝 Final WHERE clause: {where_clause}")
            logger.info(f"🔢 Total parameters: {len(entity_params)}")

            # Step 1: Get total count
            count_query = f"""
                SELECT COUNT(DISTINCT e.ccn_or_npi) 
                FROM entities_enriched e
                {where_clause}
            """
            logger.debug(f"Count query: {count_query}")
            
            count_start = asyncio.get_event_loop().time()
            count_cursor = await conn.execute(count_query, entity_params)
            total_count_row = await count_cursor.fetchone()
            total_count = total_count_row[0] if total_count_row else 0
            count_time = asyncio.get_event_loop().time() - count_start
            
            logger.info(f"📊 Total count: {total_count} (query took {count_time:.2f}s)")

            if total_count == 0:
                logger.info("❌ No results found for the given filters")
                return PaginatedEmployerResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=0,
                    total_pages=0,
                )

            # Step 2: Build the main query with proper sorting before pagination
            offset = (page - 1) * per_page
            
            # Build the actual WHERE clauses for subqueries
            def build_actual_where_clause(conditions, params):
                """Replace ? placeholders with actual values"""
                if not conditions:
                    return "1=1"
                
                actual_clause = conditions
                for i, param in enumerate(params):
                    # Escape single quotes for SQL
                    escaped_param = str(param).replace("'", "''")
                    actual_clause = actual_clause.replace(f"?", f"'{escaped_param}'", 1)
                return actual_clause

            # Build facility filters for subqueries
            facility_filters = []
            facility_params = []
            
            if cities:
                city_conditions = []
                for city in cities:
                    city_conditions.append("LOWER(facility.city) = ?")
                    facility_params.append(city.lower())
                facility_filters.append(f"({' OR '.join(city_conditions)})")
            
            if states:
                state_conditions = []
                for state in states:
                    state_conditions.append("LOWER(facility_state.state_name) = ?")
                    facility_params.append(state.lower())
                facility_filters.append(f"({' OR '.join(state_conditions)})")
            
            if address:
                facility_filters.append("LOWER(facility.address) LIKE ?")
                facility_params.append(f"%{address.lower()}%")
            
            if zipcode:
                facility_filters.append("facility.zip_code = ?")
                facility_params.append(zipcode)
            
            if types:
                type_conditions = []
                for facility_type in types:
                    type_conditions.append("LOWER(facility.type) = ?")
                    facility_params.append(facility_type.lower())
                facility_filters.append(f"({' OR '.join(type_conditions)})")
            
            if subtypes:
                subtype_conditions = []
                for subtype in subtypes:
                    subtype_conditions.append("LOWER(facility.subtype) = ?")
                    facility_params.append(subtype.lower())
                facility_filters.append(f"({' OR '.join(subtype_conditions)})")
            
            if facilities:
                facility_name_conditions = []
                for facility_name in facilities:
                    facility_name_conditions.append("LOWER(facility.name) LIKE ?")
                    facility_params.append(f"%{facility_name.lower()}%")
                facility_filters.append(f"({' OR '.join(facility_name_conditions)})")
            
            # Coordinate filter
            if coords and len(coords) >= 2:
                lats = [coord.get('lat') for coord in coords if coord.get('lat') is not None]
                lngs = [coord.get('lng') for coord in coords if coord.get('lng') is not None]
                
                if lats and lngs:
                    lat_min, lat_max = min(lats), max(lats)
                    lng_min, lng_max = min(lngs), max(lngs)
                    
                    facility_filters.append("facility.latitude BETWEEN ? AND ?")
                    facility_params.extend([lat_min, lat_max])
                    facility_filters.append("facility.longitude BETWEEN ? AND ?")
                    facility_params.extend([lng_min, lng_max])

            facility_where_clause = " AND ".join(facility_filters) if facility_filters else "1=1"

            # Build actual WHERE clauses for subqueries
            actual_combined_where = build_actual_where_clause(combined_where_clause, combined_params)
            actual_facility_where = build_actual_where_clause(facility_where_clause, facility_params)
            
            # Create versions with different aliases for nested subqueries
            actual_combined_where_nested = actual_combined_where.replace('pe.', 'pe2.').replace('pt.', 'pt2.').replace('rsc.', 'rsc2.')
            actual_combined_where_outer = actual_combined_where.replace('pe.', 'pe3.').replace('pt.', 'pt3.').replace('rsc.', 'rsc3.')
            actual_combined_where_facility = actual_combined_where.replace('pe.', 'pe4.').replace('pt.', 'pt4.').replace('rsc.', 'rsc4.')
            actual_combined_where_sort = actual_combined_where.replace('pe.', 'pe_sort.').replace('pt.', 'pt_sort.').replace('rsc.', 'rsc_sort.')

            # Build sorting logic - ensure all sort subqueries apply consistent filters
            if sort_by in ["role", "specialty"]:
                sort_field = "role" if sort_by == "role" else "specialty"
                # Apply both provider filters AND location filters when sorting
                sort_subquery = f"""
                    SELECT rsc_sort.{sort_field}
                    FROM provider_facility_employer_linked pfel_sort
                    INNER JOIN provider_employer pe_sort ON pe_sort.provider_id = pfel_sort.provider_id
                    INNER JOIN provider_taxonomies pt_sort ON pt_sort.npi = pe_sort.provider_id
                    INNER JOIN roles_specialties_classification rsc_sort ON rsc_sort.nucc_code = pt_sort.nucc_code
                    INNER JOIN entities_enriched fac_sort ON fac_sort.ccn_or_npi = pfel_sort.facility_npi_or_ccn
                    LEFT JOIN states fac_sort_state ON fac_sort_state.state_id = fac_sort.state_id
                    WHERE pfel_sort.employer_npi_or_ccn = e.ccn_or_npi
                    AND {actual_combined_where_sort}
                    AND ({actual_facility_where.replace('facility', 'fac_sort').replace('facility_state', 'fac_sort_state')})
                    ORDER BY rsc_sort.{sort_field} {sort_order}
                    LIMIT 1
                """
                order_by_clause = f"""
                    CASE WHEN ({sort_subquery}) IS NULL OR ({sort_subquery}) = '' THEN 1 ELSE 0 END,
                    ({sort_subquery}) {sort_order},
                    e.name ASC
                """
            elif sort_by in ["city", "state", "type", "subtype", "facility"]:
                sort_field_map = {
                    "city": "fac_sort.city",
                    "state": "fac_state_sort.state_name", 
                    "type": "fac_sort.type",
                    "subtype": "fac_sort.subtype",
                    "facility": "fac_sort.name"
                }
                sort_field = sort_field_map[sort_by]
                
                # Apply both provider filters AND location filters when sorting by facility attributes
                if combined_conditions:
                    # If we have provider filters, only consider facilities with matching providers
                    # Need to include state join for all queries to avoid SQL errors
                    sort_subquery = f"""
                        SELECT {sort_field}
                        FROM provider_facility_employer_linked pfel_sort
                        INNER JOIN entities_enriched fac_sort ON fac_sort.ccn_or_npi = pfel_sort.facility_npi_or_ccn
                        LEFT JOIN states fac_state_sort ON fac_state_sort.state_id = fac_sort.state_id
                        INNER JOIN provider_employer pe_sort ON pe_sort.provider_id = pfel_sort.provider_id
                        INNER JOIN provider_taxonomies pt_sort ON pt_sort.npi = pe_sort.provider_id
                        INNER JOIN roles_specialties_classification rsc_sort ON rsc_sort.nucc_code = pt_sort.nucc_code
                        WHERE pfel_sort.employer_npi_or_ccn = e.ccn_or_npi
                        AND {actual_combined_where_sort}
                        AND ({actual_facility_where.replace('facility', 'fac_sort').replace('facility_state', 'fac_state_sort')})
                        AND {sort_field} IS NOT NULL
                        ORDER BY {sort_field} {sort_order}
                        LIMIT 1
                    """
                else:
                    # No provider filters, just apply location filters
                    # Need to include state join for all queries to avoid SQL errors
                    sort_subquery = f"""
                        SELECT {sort_field}
                        FROM provider_facility_employer_linked pfel_sort
                        INNER JOIN entities_enriched fac_sort ON fac_sort.ccn_or_npi = pfel_sort.facility_npi_or_ccn
                        LEFT JOIN states fac_state_sort ON fac_state_sort.state_id = fac_sort.state_id
                        WHERE pfel_sort.employer_npi_or_ccn = e.ccn_or_npi
                        AND ({actual_facility_where.replace('facility', 'fac_sort').replace('facility_state', 'fac_state_sort')})
                        AND {sort_field} IS NOT NULL
                        ORDER BY {sort_field} {sort_order}
                        LIMIT 1
                    """
                order_by_clause = f"""
                    CASE WHEN ({sort_subquery}) IS NULL OR ({sort_subquery}) = '' THEN 1 ELSE 0 END,
                    ({sort_subquery}) {sort_order},
                    e.name ASC
                """
            elif sort_by == "providers_count":
                # Count only providers matching filters at facilities matching location filters
                sort_subquery = f"""
                    SELECT COUNT(DISTINCT pe_sort.provider_id)
                    FROM provider_facility_employer_linked pfel_sort
                    INNER JOIN provider_employer pe_sort ON pe_sort.provider_id = pfel_sort.provider_id
                    INNER JOIN provider_taxonomies pt_sort ON pt_sort.npi = pe_sort.provider_id
                    INNER JOIN roles_specialties_classification rsc_sort ON rsc_sort.nucc_code = pt_sort.nucc_code
                    INNER JOIN entities_enriched fac_sort ON fac_sort.ccn_or_npi = pfel_sort.facility_npi_or_ccn
                    LEFT JOIN states fac_sort_state ON fac_sort_state.state_id = fac_sort.state_id
                    WHERE pfel_sort.employer_npi_or_ccn = e.ccn_or_npi
                    AND {actual_combined_where_sort}
                    AND ({actual_facility_where.replace('facility', 'fac_sort').replace('facility_state', 'fac_sort_state')})
                """
                order_by_clause = f"""
                    CASE WHEN ({sort_subquery}) IS NULL OR ({sort_subquery}) = 0 THEN 1 ELSE 0 END,
                    ({sort_subquery}) {sort_order},
                    e.name ASC
                """
            else:
                order_by_clause = f"e.name {sort_order}"

            # Get sorted employer CCNs first
            sorted_ccns_query = f"""
                SELECT e.ccn_or_npi
                FROM entities_enriched e
                {where_clause}
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
            """

            # Execute to get sorted CCNs first
            ccn_params = entity_params + [per_page, offset]
            ccn_start_time = asyncio.get_event_loop().time()
            ccn_cursor = await conn.execute(sorted_ccns_query, ccn_params)
            ccn_rows = await ccn_cursor.fetchall()
            ccns = [row[0] for row in ccn_rows]
            ccn_query_time = asyncio.get_event_loop().time() - ccn_start_time

            logger.info(f"✅ Sorted CCNs query executed in {ccn_query_time:.2f}s, found {len(ccns)} employers")

            total_pages = (total_count + per_page - 1) // per_page

            if not ccns:
                logger.info("❌ No results found after pagination")
                return PaginatedEmployerResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=total_count,
                    total_pages=total_pages,
                )

            # Now fetch the basic employer data for the sorted CCNs
            placeholders = ','.join(['?' for _ in ccns])
            main_query = f"""
                SELECT
                    e.id,
                    e.name,
                    e.ccn_or_npi
                FROM entities_enriched e
                WHERE e.ccn_or_npi IN ({placeholders})
            """

            # Execute main query with CCNs as parameters
            main_start_time = asyncio.get_event_loop().time()
            cursor = await conn.execute(main_query, ccns)
            rows = await cursor.fetchall()
            main_query_time = asyncio.get_event_loop().time() - main_start_time

            logger.info(f"✅ Main data query executed in {main_query_time:.2f}s, found {len(rows)} rows")

            # Create a mapping of ccn to index for sorting to maintain the order
            ccn_to_index = {ccn: idx for idx, ccn in enumerate(ccns)}
            rows_sorted = sorted(rows, key=lambda row: ccn_to_index[row['ccn_or_npi']])

            # Convert rows to basic employers
            basic_employers = []
            for row in rows_sorted:
                row_dict = dict(row)
                if row_dict.get('name'):
                    row_dict['name'] = to_title_case(row_dict['name'])
                basic_employers.append(row_dict)

            # Execute subqueries for each employer
            employers = []
            logger.info(f"🔄 Starting subquery processing for {len(basic_employers)} employers...")
            
            for i, employer in enumerate(basic_employers):
                employer_ccn = employer['ccn_or_npi']
                
                # Determine sorting order for facilities and related arrays based on sort_by
                if sort_by == "city":
                    facilities_order = f"ORDER BY facility.city {sort_order}, facility.name ASC"
                    cities_order = f"ORDER BY facility.city {sort_order}"
                elif sort_by == "state":
                    facilities_order = f"ORDER BY facility_state.state_name {sort_order}, facility.name ASC"
                    states_order = f"ORDER BY facility_state.state_name {sort_order}"
                elif sort_by == "type":
                    facilities_order = f"ORDER BY facility.type {sort_order}, facility.name ASC"
                    types_order = f"ORDER BY facility.type {sort_order}, facility.subtype ASC"
                elif sort_by == "subtype":
                    facilities_order = f"ORDER BY facility.subtype {sort_order}, facility.name ASC"
                    types_order = f"ORDER BY facility.subtype {sort_order}, facility.type ASC"
                elif sort_by == "facility":
                    facilities_order = f"ORDER BY facility.name {sort_order}"
                else:
                    # Default sorting for facilities and arrays
                    facilities_order = "ORDER BY facility.name ASC"
                    cities_order = "ORDER BY facility.city ASC"
                    states_order = "ORDER BY facility_state.state_name ASC"
                    types_order = "ORDER BY facility.type ASC, facility.subtype ASC"
                
                # Set default orderings if not set
                if sort_by != "city":
                    cities_order = "ORDER BY facility.city ASC"
                if sort_by not in ["state"]:
                    states_order = "ORDER BY facility_state.state_name ASC"
                if sort_by not in ["type", "subtype"]:
                    types_order = "ORDER BY facility.type ASC, facility.subtype ASC"
                
                # Define subqueries to run in parallel
                subqueries = [
                    # Providers count - ONLY at facilities matching location filters AND role/specialty
                    (
                        f"""
                        SELECT COUNT(DISTINCT pe.provider_id)
                        FROM provider_facility_employer_linked pfel
                        INNER JOIN provider_employer pe ON pe.provider_id = pfel.provider_id
                        INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                        INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                        INNER JOIN entities_enriched fac_filter ON fac_filter.ccn_or_npi = pfel.facility_npi_or_ccn
                        LEFT JOIN states fac_filter_state ON fac_filter_state.state_id = fac_filter.state_id
                        WHERE pfel.employer_npi_or_ccn = ?
                        AND {actual_combined_where}
                        AND ({facility_where_clause.replace('facility', 'fac_filter').replace('facility_state', 'fac_filter_state')})
                        """,
                        [employer_ccn] + facility_params,
                        "providers_count"
                    ),
                    # Roles - ONLY from providers at facilities matching location filters AND role/specialty
                    (
                        f"""
                        SELECT json_group_array(COALESCE(role, ''))
                        FROM (
                            SELECT DISTINCT rsc.role
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN provider_employer pe ON pe.provider_id = pfel.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            INNER JOIN entities_enriched fac_filter ON fac_filter.ccn_or_npi = pfel.facility_npi_or_ccn
                            LEFT JOIN states fac_filter_state ON fac_filter_state.state_id = fac_filter.state_id
                            WHERE pfel.employer_npi_or_ccn = ?
                            AND {actual_combined_where}
                            AND ({facility_where_clause.replace('facility', 'fac_filter').replace('facility_state', 'fac_filter_state')})
                            ORDER BY rsc.role ASC
                        )
                        """,
                        [employer_ccn] + facility_params,
                        "roles"
                    ),
                    # Specialties - ONLY from providers at facilities matching location filters AND role/specialty
                    (
                        f"""
                        SELECT json_group_array(COALESCE(specialty, ''))
                        FROM (
                            SELECT DISTINCT rsc.specialty
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN provider_employer pe ON pe.provider_id = pfel.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            INNER JOIN entities_enriched fac_filter ON fac_filter.ccn_or_npi = pfel.facility_npi_or_ccn
                            LEFT JOIN states fac_filter_state ON fac_filter_state.state_id = fac_filter.state_id
                            WHERE pfel.employer_npi_or_ccn = ?
                            AND {actual_combined_where}
                            AND ({facility_where_clause.replace('facility', 'fac_filter').replace('facility_state', 'fac_filter_state')})
                            ORDER BY rsc.specialty ASC
                        )
                        """,
                        [employer_ccn] + facility_params,
                        "specialties"
                    ),
                    # Facilities with provider count - ONLY facilities with providers matching role/specialty
                    (
                        f"""
                        SELECT COALESCE(
                            json_group_array(
                                json_object(
                                    'id', id,
                                    'name', name,
                                    'type', type,
                                    'subtype', subtype,
                                    'city', city,
                                    'state_name', state_name,
                                    'state_code', state_code,
                                    'address', address,
                                    'zip_code', zip_code,
                                    'latitude', latitude,
                                    'longitude', longitude,
                                    'ccn_or_npi', ccn_or_npi,
                                    'provider_count', provider_count
                                )
                            ),
                            '[]'
                        )
                        FROM (
                            SELECT 
                                facility.id,
                                COALESCE(facility.name, '') as name,
                                COALESCE(facility.type, '') as type,
                                COALESCE(facility.subtype, '') as subtype,
                                COALESCE(facility.city, '') as city,
                                COALESCE(facility_state.state_name, '') as state_name,
                                COALESCE(facility_state.state_code, '') as state_code,
                                COALESCE(facility.address, '') as address,
                                COALESCE(facility.zip_code, '') as zip_code,
                                COALESCE(facility.latitude, 0.0) as latitude,
                                COALESCE(facility.longitude, 0.0) as longitude,
                                COALESCE(facility.ccn_or_npi, '') as ccn_or_npi,
                                (
                                    SELECT COUNT(DISTINCT pe2.provider_id)
                                    FROM provider_facility_employer_linked pfel2
                                    INNER JOIN provider_employer pe2 ON pe2.provider_id = pfel2.provider_id
                                    INNER JOIN provider_taxonomies pt2 ON pt2.npi = pe2.provider_id
                                    INNER JOIN roles_specialties_classification rsc2 ON rsc2.nucc_code = pt2.nucc_code
                                    WHERE pfel2.facility_npi_or_ccn = facility.ccn_or_npi
                                    AND pfel2.employer_npi_or_ccn = ?
                                    AND {actual_combined_where_nested}
                                ) as provider_count
                            FROM entities_enriched facility
                            LEFT JOIN states facility_state ON facility_state.state_id = facility.state_id
                            WHERE facility.ccn_or_npi IN (
                                SELECT DISTINCT pfel3.facility_npi_or_ccn
                                FROM provider_facility_employer_linked pfel3
                                WHERE pfel3.employer_npi_or_ccn = ?
                            )
                            AND EXISTS (
                                SELECT 1 FROM provider_facility_employer_linked pfel4
                                INNER JOIN provider_employer pe4 ON pe4.provider_id = pfel4.provider_id
                                INNER JOIN provider_taxonomies pt4 ON pt4.npi = pe4.provider_id
                                INNER JOIN roles_specialties_classification rsc4 ON rsc4.nucc_code = pt4.nucc_code
                                WHERE pfel4.facility_npi_or_ccn = facility.ccn_or_npi
                                AND pfel4.employer_npi_or_ccn = ?
                                AND {actual_combined_where_facility}
                            )
                            AND {facility_where_clause}
                            {facilities_order}
                        )
                        """,
                        [employer_ccn, employer_ccn, employer_ccn] + facility_params,
                        "facilities"
                    ),
                    # Facility cities - ONLY from facilities with providers matching role/specialty
                    (
                        f"""
                        SELECT COALESCE(json_group_array(facility_city), '[]')
                        FROM (
                            SELECT DISTINCT facility.city as facility_city
                            FROM entities_enriched facility
                            LEFT JOIN states facility_state ON facility_state.state_id = facility.state_id
                            WHERE facility.ccn_or_npi IN (
                                SELECT DISTINCT pfel.facility_npi_or_ccn
                                FROM provider_facility_employer_linked pfel
                                WHERE pfel.employer_npi_or_ccn = ?
                            )
                            AND EXISTS (
                                SELECT 1 FROM provider_facility_employer_linked pfel4
                                INNER JOIN provider_employer pe4 ON pe4.provider_id = pfel4.provider_id
                                INNER JOIN provider_taxonomies pt4 ON pt4.npi = pe4.provider_id
                                INNER JOIN roles_specialties_classification rsc4 ON rsc4.nucc_code = pt4.nucc_code
                                WHERE pfel4.facility_npi_or_ccn = facility.ccn_or_npi
                                AND pfel4.employer_npi_or_ccn = ?
                                AND {actual_combined_where_facility}
                            )
                            AND {facility_where_clause}
                            AND facility.city IS NOT NULL
                            AND facility.city != ''
                            {cities_order}
                        )
                        """,
                        [employer_ccn, employer_ccn] + facility_params,
                        "facility_cities"
                    ),
                    # Facility states - ONLY from facilities with providers matching role/specialty
                    (
                        f"""
                        SELECT COALESCE(
                            json_group_array(
                                json_object('state_name', state_name, 'state_code', state_code)
                            ),
                            '[]'
                        )
                        FROM (
                            SELECT DISTINCT facility_state.state_name, facility_state.state_code
                            FROM entities_enriched facility
                            LEFT JOIN states facility_state ON facility_state.state_id = facility.state_id
                            WHERE facility.ccn_or_npi IN (
                                SELECT DISTINCT pfel.facility_npi_or_ccn
                                FROM provider_facility_employer_linked pfel
                                WHERE pfel.employer_npi_or_ccn = ?
                            )
                            AND EXISTS (
                                SELECT 1 FROM provider_facility_employer_linked pfel4
                                INNER JOIN provider_employer pe4 ON pe4.provider_id = pfel4.provider_id
                                INNER JOIN provider_taxonomies pt4 ON pt4.npi = pe4.provider_id
                                INNER JOIN roles_specialties_classification rsc4 ON rsc4.nucc_code = pt4.nucc_code
                                WHERE pfel4.facility_npi_or_ccn = facility.ccn_or_npi
                                AND pfel4.employer_npi_or_ccn = ?
                                AND {actual_combined_where_facility}
                            )
                            AND {facility_where_clause}
                            AND facility_state.state_name IS NOT NULL
                            {states_order}
                        )
                        """,
                        [employer_ccn, employer_ccn] + facility_params,
                        "facility_states"
                    ),
                    # Facility types - ONLY from facilities with providers matching role/specialty
                    (
                        f"""
                        SELECT COALESCE(
                            json_group_array(
                                json_object('type', facility_type, 'subtype', facility_subtype)
                            ),
                            '[]'
                        )
                        FROM (
                            SELECT DISTINCT 
                                COALESCE(facility.type, '') as facility_type,
                                COALESCE(facility.subtype, '') as facility_subtype
                            FROM entities_enriched facility
                            LEFT JOIN states facility_state ON facility_state.state_id = facility.state_id
                            WHERE facility.ccn_or_npi IN (
                                SELECT DISTINCT pfel.facility_npi_or_ccn
                                FROM provider_facility_employer_linked pfel
                                WHERE pfel.employer_npi_or_ccn = ?
                            )
                            AND EXISTS (
                                SELECT 1 FROM provider_facility_employer_linked pfel4
                                INNER JOIN provider_employer pe4 ON pe4.provider_id = pfel4.provider_id
                                INNER JOIN provider_taxonomies pt4 ON pt4.npi = pe4.provider_id
                                INNER JOIN roles_specialties_classification rsc4 ON rsc4.nucc_code = pt4.nucc_code
                                WHERE pfel4.facility_npi_or_ccn = facility.ccn_or_npi
                                AND pfel4.employer_npi_or_ccn = ?
                                AND {actual_combined_where_facility}
                            )
                            AND {facility_where_clause}
                            AND facility.type IS NOT NULL
                            {types_order}
                        )
                        """,
                        [employer_ccn, employer_ccn] + facility_params,
                        "facility_types"
                    )
                ]
                
                # Execute all subqueries in parallel
                start_subqueries = asyncio.get_event_loop().time()
                tasks = [execute_subquery(conn, query, params, field) for query, params, field in subqueries]
                results = await asyncio.gather(*tasks)
                subquery_time = asyncio.get_event_loop().time() - start_subqueries
                
                # Combine results - FIXED: Properly handle empty arrays
                employer_data = employer.copy()
                for field_name, result in results:
                    if field_name == "facilities":
                        logger.debug(f"🏥 Facilities raw result for {employer_ccn}: {result}")
                        if result:
                            try:
                                facilities_data = json.loads(result)
                                logger.debug(f"🏥 Facilities parsed data type: {type(facilities_data)}, length: {len(facilities_data) if isinstance(facilities_data, list) else 'N/A'}")
                                
                                # Filter out None/null values that might be in the array
                                if isinstance(facilities_data, list):
                                    facilities_data = [f for f in facilities_data if f is not None]
                                
                                for facility in facilities_data:
                                    if facility.get('name'):
                                        facility['name'] = to_title_case(facility['name'])
                                    if facility.get('city'):
                                        facility['city'] = to_title_case(facility['city'])
                                    if facility.get('address'):
                                        facility['address'] = to_title_case(facility['address'])
                                employer_data[field_name] = facilities_data
                                logger.debug(f"🏥 Final facilities count: {len(facilities_data)}")
                            except Exception as e:
                                logger.error(f"🚨 Error parsing facilities JSON: {e}")
                                employer_data[field_name] = []
                        else:
                            logger.debug(f"🏥 Facilities result is empty/None for {employer_ccn}")
                            employer_data[field_name] = []
                    elif field_name in ["roles", "specialties", "facility_cities"]:
                        if result:
                            parsed_data = json.loads(result)
                            if field_name == "facility_cities":
                                # Convert city names to title case
                                employer_data[field_name] = [to_title_case(city) for city in parsed_data if city]
                            else:
                                employer_data[field_name] = parsed_data
                        else:
                            employer_data[field_name] = []
                    elif field_name in ["facility_states", "facility_types"]:
                        if result:
                            employer_data[field_name] = json.loads(result)
                        else:
                            employer_data[field_name] = []
                    elif field_name == "providers_count":
                        employer_data[field_name] = result if result is not None else 0
                    else:
                        employer_data[field_name] = result if result is not None else []
                
                employers.append(EmployerResponse(**employer_data))
                
                if (i + 1) % 5 == 0:  # Log progress every 5 employers
                    logger.info(f"📦 Processed {i + 1}/{len(basic_employers)} employers, last batch subqueries took {subquery_time:.2f}s")

            total_processing_time = asyncio.get_event_loop().time() - count_start
            logger.info(f"🎉 Query completed successfully in {total_processing_time:.2f}s")
            logger.info(f"📊 Final result: {len(employers)} employers, {total_count} total, {total_pages} pages")

            return PaginatedEmployerResponse(
                data=employers,
                page=page,
                per_page=per_page,
                total=total_count,
                total_pages=total_pages,
            )

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"💥 Database error details: {error_details}")
        logger.error(f"🚨 Container User Info during error - UID: {os.getuid()}, GID: {os.getgid()}")
        logger.error(f"📁 Current working directory: {os.getcwd()}")
        logger.error(f"🔍 Database path environment: {os.getenv('DATABASE_PATH', 'Not set')}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")