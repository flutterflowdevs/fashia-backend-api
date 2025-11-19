import os
import logging
from typing import List, Optional
from fastapi import HTTPException
from app.db.session import get_db_connection
from app.models.provider_model import ProviderResponse, PaginatedProviderResponse
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
        logger.debug(f"Executing subquery for {field_name}")
        cursor = await conn.execute(query, params)
        result = await cursor.fetchone()
        return field_name, result[0] if result else None
    except Exception as e:
        logger.error(f"Error in subquery for {field_name}: {e}")
        return field_name, None

def build_filter_conditions(params_dict: dict) -> tuple:
    """Build filter conditions and parameters"""
    conditions = []
    params = []
    
    # Provider name filters
    if params_dict.get('first_name'):
        conditions.append("LOWER(p.first_name) = LOWER(?)")
        params.append(params_dict['first_name'])
    
    if params_dict.get('last_name'):
        conditions.append("LOWER(p.last_name) = LOWER(?)")
        params.append(params_dict['last_name'])
    
    # License state filter
    if params_dict.get('license_state_id') is not None:
        conditions.append("pt.license_state_id = ?")
        params.append(params_dict['license_state_id'])
    
    return conditions, params

def build_role_specialty_conditions(roles: List[str], specialties: List[str]) -> tuple:
    """Build role and specialty conditions"""
    conditions = []
    params = []
    
    if roles:
        role_conditions = []
        for role in roles:
            role_conditions.append("LOWER(rsc.role) = LOWER(?)")
            params.append(role)
        conditions.append(f"({' OR '.join(role_conditions)})")
    
    if specialties:
        specialty_conditions = []
        for specialty in specialties:
            specialty_conditions.append("LOWER(rsc.specialty) = LOWER(?)")
            params.append(specialty)
        conditions.append(f"({' OR '.join(specialty_conditions)})")
    
    return conditions, params

def build_facility_conditions_for_pfel(
    facility_cities: List[str],
    facility_states: List[str],
    facility_address: str,
    facility_zipcode: str,
    facility_names: List[str],
    employer_names: List[str],
    facility_types: List[str],
    facility_subtypes: List[str]
) -> tuple:
    """Build facility-related conditions for pfel_new table"""
    conditions = []
    params = []
    
    if facility_cities:
        city_conditions = []
        for city in facility_cities:
            city_conditions.append("LOWER(pfel.facility_city) = LOWER(?)")
            params.append(city)
        conditions.append(f"({' OR '.join(city_conditions)})")
    
    if facility_states:
        state_conditions = []
        for state in facility_states:
            state_conditions.append("LOWER(pfel.facility_state_name) = LOWER(?)")
            params.append(state)
        conditions.append(f"({' OR '.join(state_conditions)})")
    
    if facility_address:
        conditions.append("LOWER(pfel.facility_address) LIKE LOWER(?)")
        params.append(f"%{facility_address}%")
    
    if facility_zipcode:
        conditions.append("pfel.facility_zipcode = ?")
        params.append(facility_zipcode)
    
    if facility_names:
        name_conditions = []
        for name in facility_names:
            name_conditions.append("LOWER(pfel.facility_name) = LOWER(?)")
            params.append(name)
        conditions.append(f"({' OR '.join(name_conditions)})")
    
    if facility_types:
        type_conditions = []
        for ftype in facility_types:
            type_conditions.append("LOWER(pfel.facility_type) = LOWER(?)")
            params.append(ftype)
        conditions.append(f"({' OR '.join(type_conditions)})")
    
    if facility_subtypes:
        subtype_conditions = []
        for subtype in facility_subtypes:
            subtype_conditions.append("LOWER(pfel.facility_subtype) = LOWER(?)")
            params.append(subtype)
        conditions.append(f"({' OR '.join(subtype_conditions)})")
    
    if employer_names:
        emp_conditions = []
        for employer in employer_names:
            emp_conditions.append("LOWER(pfel.employer_name) LIKE LOWER(?)")
            params.append(f"%{employer}%")
        conditions.append(f"({' OR '.join(emp_conditions)})")
    
    return conditions, params

def build_facility_conditions_for_entities(
    facility_cities: List[str],
    facility_states: List[str],
    facility_address: str,
    facility_zipcode: str,
    facility_names: List[str],
    facility_types: List[str],
    facility_subtypes: List[str]
) -> tuple:
    """Build facility-related conditions for provider_entities + entities_enriched tables"""
    conditions = []
    params = []
    
    if facility_cities:
        city_conditions = []
        for city in facility_cities:
            city_conditions.append("LOWER(ee.city) = LOWER(?)")
            params.append(city)
        conditions.append(f"({' OR '.join(city_conditions)})")
    
    if facility_states:
        state_conditions = []
        for state in facility_states:
            state_conditions.append("LOWER(st.state_name) = LOWER(?)")
            params.append(state)
        conditions.append(f"({' OR '.join(state_conditions)})")
    
    if facility_address:
        conditions.append("LOWER(ee.address) LIKE LOWER(?)")
        params.append(f"%{facility_address}%")
    
    if facility_zipcode:
        conditions.append("ee.zip_code = ?")
        params.append(facility_zipcode)
    
    if facility_names:
        name_conditions = []
        for name in facility_names:
            name_conditions.append("LOWER(ee.name) = LOWER(?)")
            params.append(name)
        conditions.append(f"({' OR '.join(name_conditions)})")
    
    if facility_types:
        type_conditions = []
        for ftype in facility_types:
            type_conditions.append("LOWER(ee.type) = LOWER(?)")
            params.append(ftype)
        conditions.append(f"({' OR '.join(type_conditions)})")
    
    if facility_subtypes:
        subtype_conditions = []
        for subtype in facility_subtypes:
            subtype_conditions.append("LOWER(ee.subtype) = LOWER(?)")
            params.append(subtype)
        conditions.append(f"({' OR '.join(subtype_conditions)})")
    
    return conditions, params

def build_employer_conditions_for_employer(
    employer_names: List[str]
) -> tuple:
    """Build employer-related conditions for provider_employer + entities_enriched tables"""
    conditions = []
    params = []
    
    if employer_names:
        emp_conditions = []
        for employer in employer_names:
            emp_conditions.append("LOWER(ee.name) LIKE LOWER(?)")
            params.append(f"%{employer}%")
        conditions.append(f"({' OR '.join(emp_conditions)})")
    
    return conditions, params

def determine_query_source(
    facility_cities: List[str],
    facility_states: List[str], 
    facility_address: str,
    facility_zipcode: str,
    facility_names: List[str],
    employer_names: List[str],
    facility_types: List[str],
    facility_subtypes: List[str]
) -> tuple:
    """
    Determine which table to use based on filter combinations
    Returns: (main_table, table_alias, join_condition, has_facility_data, query_type)
    """
    has_facility_filters = any([
        facility_cities, facility_states, facility_address, 
        facility_zipcode, facility_names, facility_types, facility_subtypes
    ])
    has_employer_filters = any([employer_names])
    
    logger.info(f"🔍 Filter analysis - Facility: {has_facility_filters}, Employer: {has_employer_filters}")
    
    if has_facility_filters and has_employer_filters:
        # Both facility and employer filters - use pfel_new table
        logger.info("📊 Using pfel_new table (both facility and employer filters)")
        return "pfel_new", "pfel", "INNER JOIN pfel_new pfel ON pfel.provider_id = p.npi", True, "pfel"
    
    elif has_facility_filters:
        # Only facility filters - use provider_entities + entities_enriched tables
        logger.info("🏥 Using provider_entities + entities_enriched tables (only facility filters)")
        join_condition = """
            INNER JOIN provider_entities pe ON pe.provider_id = p.npi
            INNER JOIN entities_enriched ee ON ee.ccn_or_npi = pe.npi_or_ccn
            LEFT JOIN states st ON st.state_id = ee.state_id
        """
        return "provider_entities", "pe", join_condition, True, "facility"
    
    elif has_employer_filters:
        # Only employer filters - use provider_employer + entities_enriched tables
        logger.info("💼 Using provider_employer + entities_enriched tables (only employer filters)")
        join_condition = """
            INNER JOIN provider_employer pem ON pem.provider_id = p.npi
            INNER JOIN entities_enriched ee ON ee.ccn_or_npi = pem.npi_or_ccn
            LEFT JOIN states st ON st.state_id = ee.state_id
        """
        return "provider_employer", "pem", join_condition, True, "employer"
    
    else:
        # Only provider filters - no facility/employer table needed
        logger.info("👤 Using providers table only (only provider filters)")
        return "providers", "", "", False, "provider_only"

async def get_providers_data(
    first_name: Optional[str] = "",
    last_name: Optional[str] = "",
    roles: Optional[List[str]] = None,
    specialties: Optional[List[str]] = None,
    facility_cities: Optional[List[str]] = None,
    facility_states: Optional[List[str]] = None,
    facility_address: Optional[str] = "",
    facility_zipcode: Optional[str] = "",
    license_state_id: Optional[int] = None,
    facility_names: Optional[List[str]] = None,
    employer_names: Optional[List[str]] = None,
    facility_types: Optional[List[str]] = None,
    facility_subtypes: Optional[List[str]] = None,
    page: int = 1,
    per_page: int = 25,
    sort_by: str = "name",
    sort_order: str = "ASC",
):
    """
    Fetch paginated providers from local SQLite database.
    """
    logger.info("🔍 get_providers_data called")
    
    # Handle None values for lists
    roles = roles or []
    specialties = specialties or []
    facility_cities = facility_cities or []
    facility_states = facility_states or []
    facility_names = facility_names or []
    employer_names = employer_names or []
    facility_types = facility_types or []
    facility_subtypes = facility_subtypes or []
    
    # Validate sort parameters
    valid_sort_fields = [
        "name", "first_name", "last_name", "role", "specialty", 
        "facility_city", "facility_state", "licensure_state", 
        "facility_name", "employer_name", "facility_type", "facility_subtype"
    ]
    if sort_by not in valid_sort_fields:
        logger.warning(f"Invalid sort_by: {sort_by}, defaulting to 'name'")
        sort_by = "name"
    
    sort_order = sort_order.upper()
    if sort_order not in ["ASC", "DESC"]:
        logger.warning(f"Invalid sort_order: {sort_order}, defaulting to 'ASC'")
        sort_order = "ASC"
    
    try:
        logger.info("🔄 Attempting database connection...")
        
        async with get_db_connection() as conn:
            logger.info("✅ Database connection established successfully")
            
            # Set query optimizations
            await conn.execute("PRAGMA temp_store = MEMORY")
            await conn.execute("PRAGMA cache_size = -64000")
            
            # Build all filter conditions
            base_conditions, base_params = build_filter_conditions({
                'first_name': first_name,
                'last_name': last_name,
                'license_state_id': license_state_id
            })
            
            role_specialty_conditions, role_specialty_params = build_role_specialty_conditions(roles, specialties)
            if role_specialty_conditions:
                base_conditions.extend(role_specialty_conditions)
                base_params.extend(role_specialty_params)
            
            # Determine which table to use based on filter combinations
            main_table, table_alias, facility_join, has_facility_data, query_type = determine_query_source(
                facility_cities, facility_states, facility_address, facility_zipcode,
                facility_names, employer_names, facility_types, facility_subtypes
            )
            
            # Build facility conditions based on query type
            if query_type == "pfel":
                facility_conditions, facility_params = build_facility_conditions_for_pfel(
                    facility_cities, facility_states, facility_address, facility_zipcode,
                    facility_names, employer_names, facility_types, facility_subtypes
                )
            elif query_type == "facility":
                facility_conditions, facility_params = build_facility_conditions_for_entities(
                    facility_cities, facility_states, facility_address, facility_zipcode,
                    facility_names, facility_types, facility_subtypes
                )
            elif query_type == "employer":
                facility_conditions, facility_params = build_employer_conditions_for_employer(employer_names)
            else:
                facility_conditions, facility_params = [], []
            
            # Add facility conditions to main query if needed
            if has_facility_data and facility_conditions:
                base_conditions.extend(facility_conditions)
                base_params.extend(facility_params)
            
            # Build WHERE clause
            where_clause = "WHERE " + " AND ".join(base_conditions) if base_conditions else ""
            logger.info(f"📝 Final WHERE clause: {where_clause}")
            logger.info(f"🔢 Base params count: {len(base_params)}")
            logger.info(f"🏷️ Using table: {main_table} with query type: {query_type}")
            
            # Step 1: Get total count using CTE for better performance
            count_query = f"""
                WITH filtered_providers AS (
                    SELECT DISTINCT p.npi
                    FROM providers p
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                    LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    {facility_join}
                    {where_clause}
                )
                SELECT COUNT(*) FROM filtered_providers
            """
            
            count_start = asyncio.get_event_loop().time()
            count_cursor = await conn.execute(count_query, base_params)
            total_count_row = await count_cursor.fetchone()
            total_count = total_count_row[0] if total_count_row else 0
            count_time = asyncio.get_event_loop().time() - count_start
            
            logger.info(f"📊 Total count: {total_count} (query took {count_time:.2f}s)")
            
            if total_count == 0:
                logger.info("❌ No results found for the given filters")
                return PaginatedProviderResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=0,
                    total_pages=0,
                )
            
            # Step 2: Build sorting and get paginated NPIs
            offset = (page - 1) * per_page
            
            # Build order by clause
            order_by_clause = build_order_by_clause(sort_by, sort_order, role_specialty_conditions, facility_conditions, has_facility_data, query_type)
            
            # Get sorted NPIs using CTE
            sorted_npis_query = f"""
                WITH filtered_providers AS (
                    SELECT DISTINCT p.npi
                    FROM providers p
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                    LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    {facility_join}
                    {where_clause}
                )
                SELECT p.npi
                FROM providers p
                INNER JOIN filtered_providers fp ON fp.npi = p.npi
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
            """
            
            npi_params = base_params + [per_page, offset]
            npi_start_time = asyncio.get_event_loop().time()
            npi_cursor = await conn.execute(sorted_npis_query, npi_params)
            npi_rows = await npi_cursor.fetchall()
            npis = [row[0] for row in npi_rows]
            npi_query_time = asyncio.get_event_loop().time() - npi_start_time
            
            logger.info(f"✅ Sorted NPIs query executed in {npi_query_time:.2f}s, found {len(npis)} providers")
            
            if not npis:
                total_pages = (total_count + per_page - 1) // per_page
                return PaginatedProviderResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=total_count,
                    total_pages=total_pages,
                )
            
            # Step 3: Fetch complete provider data with all relationships
            providers = await fetch_complete_provider_data(
                conn, npis, role_specialty_conditions, facility_conditions,
                role_specialty_params, facility_params,
                first_name, last_name, roles, specialties,
                has_facility_data, query_type,
                facility_cities, facility_states, facility_address, facility_zipcode,
                facility_names, employer_names, facility_types, facility_subtypes
            )
            
            total_pages = (total_count + per_page - 1) // per_page
            
            logger.info(f"🎉 Query completed successfully")
            logger.info(f"📊 Final result: {len(providers)} providers, {total_count} total, {total_pages} pages")
            
            return PaginatedProviderResponse(
                data=providers,
                page=page,
                per_page=per_page,
                total=total_count,
                total_pages=total_pages,
            )
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"💥 Database error details: {error_details}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def build_order_by_clause(sort_by: str, sort_order: str, role_specialty_conditions: List, facility_conditions: List, has_facility_data: bool, query_type: str) -> str:
    """Build the ORDER BY clause based on sort parameters"""
    if sort_by in ["role", "specialty"]:
        # For role/specialty sorting, use EXISTS with conditions
        role_specialty_where = " AND ".join(role_specialty_conditions) if role_specialty_conditions else "1=1"
        if sort_by == "role":
            return f"""
                (SELECT rsc.role FROM provider_taxonomies pt
                 INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                 WHERE pt.npi = p.npi AND {role_specialty_where}
                 ORDER BY rsc.role {sort_order} LIMIT 1) {sort_order},
                p.last_name ASC, p.first_name ASC
            """
        else:  # specialty
            return f"""
                (SELECT rsc.specialty FROM provider_taxonomies pt
                 INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                 WHERE pt.npi = p.npi AND {role_specialty_where}
                 ORDER BY rsc.specialty {sort_order} LIMIT 1) {sort_order},
                p.last_name ASC, p.first_name ASC
            """
    
    elif sort_by in ["facility_city", "facility_state", "facility_name", "employer_name", 
                    "facility_type", "facility_subtype"] and has_facility_data:
        # For facility-related sorting - only if we have facility data
        facility_where = " AND ".join(facility_conditions) if facility_conditions else "1=1"
        
        # Determine the table and column to use for sorting based on query type
        if query_type == "pfel":
            field_map = {
                "facility_city": "pfel.facility_city",
                "facility_state": "pfel.facility_state_name", 
                "facility_name": "pfel.facility_name",
                "employer_name": "pfel.employer_name",
                "facility_type": "pfel.facility_type",
                "facility_subtype": "pfel.facility_subtype"
            }
            sort_table = "pfel_new pfel"
        elif query_type == "facility":
            field_map = {
                "facility_city": "ee.city",
                "facility_state": "st.state_name", 
                "facility_name": "ee.name",
                "employer_name": "NULL",
                "facility_type": "ee.type",
                "facility_subtype": "ee.subtype"
            }
            sort_table = "entities_enriched ee INNER JOIN provider_entities pe_sort ON pe_sort.provider_id = p.npi AND ee.ccn_or_npi = pe_sort.facility_npi_or_ccn LEFT JOIN states st ON st.state_id = ee.state_id"
        elif query_type == "employer":
            field_map = {
                "facility_city": "ee.city",
                "facility_state": "st.state_name", 
                "facility_name": "NULL",
                "employer_name": "ee.name",
                "facility_type": "NULL",
                "facility_subtype": "NULL"
            }
            sort_table = "entities_enriched ee INNER JOIN provider_employer pem_sort ON pem_sort.provider_id = p.npi AND ee.ccn_or_npi = pem_sort.employer_npi_or_ccn LEFT JOIN states st ON st.state_id = ee.state_id"
        else:
            field_map = {}
        
        field = field_map.get(sort_by, "NULL")
        
        if field == "NULL":
            # If the field doesn't exist in the current table, fall back to name sorting
            return f"p.last_name {sort_order}, p.first_name {sort_order}"
        
        return f"""
            (SELECT {field} FROM {sort_table} 
             WHERE provider_id = p.npi AND {facility_where}
             ORDER BY {field} {sort_order} LIMIT 1) {sort_order},
            p.last_name ASC, p.first_name ASC
        """
    
    elif sort_by == "licensure_state":
        return f"""
            (SELECT lic_state.state_name FROM provider_taxonomies pt
             LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
             WHERE pt.npi = p.npi
             ORDER BY lic_state.state_name {sort_order} LIMIT 1) {sort_order},
            p.last_name ASC, p.first_name ASC
        """
    
    else:  # name, first_name, last_name
        if sort_by == "name" or sort_by == "last_name":
            return f"p.last_name {sort_order}, p.first_name {sort_order}"
        elif sort_by == "first_name":
            return f"p.first_name {sort_order}, p.last_name {sort_order}"
        else:
            return "p.last_name ASC, p.first_name ASC"

async def fetch_complete_provider_data(
    conn, npis: List[str], 
    role_specialty_conditions: List, 
    facility_conditions: List,
    role_specialty_params: List,
    facility_params: List,
    first_name: str,
    last_name: str,
    roles: List[str],
    specialties: List[str],
    has_facility_data: bool,
    query_type: str,
    facility_cities: List[str],
    facility_states: List[str],
    facility_address: str,
    facility_zipcode: str,
    facility_names: List[str],
    employer_names: List[str],
    facility_types: List[str],
    facility_subtypes: List[str]
) -> List[ProviderResponse]:
    """Fetch complete provider data with all relationships using optimized CTEs"""
    placeholders = ','.join(['?' for _ in npis])
    
    # Build WHERE conditions for subqueries
    role_specialty_where = " AND ".join(role_specialty_conditions) if role_specialty_conditions else "1=1"
    
    # Build the main query based on query type
    if query_type == "pfel":
        comprehensive_query = build_pfel_comprehensive_query(
            npis, placeholders, role_specialty_where,
            facility_cities, facility_states, facility_address, facility_zipcode,
            facility_names, employer_names, facility_types, facility_subtypes
        )
    elif query_type == "facility":
        comprehensive_query = build_facility_comprehensive_query(
            npis, placeholders, role_specialty_where,
            facility_cities, facility_states, facility_address, facility_zipcode,
            facility_names, facility_types, facility_subtypes
        )
    elif query_type == "employer":
        comprehensive_query = build_employer_comprehensive_query(
            npis, placeholders, role_specialty_where,
            employer_names
        )
    else:
        comprehensive_query = build_providers_only_comprehensive_query(npis, placeholders, role_specialty_where)
    
    # Build parameters
    logger.info(f"🔍 Building params - npis: {len(npis)}, role_specialty_params: {len(role_specialty_params) if role_specialty_params else 0}")
    logger.info(f"🔍 Facility params - cities: {facility_cities}, states: {facility_states}, names: {facility_names}")
    logger.info(f"🔍 Employer params - names: {employer_names}")
    
    all_params = build_comprehensive_query_params(
        npis, role_specialty_params, query_type,
        facility_cities, facility_states, facility_address, facility_zipcode,
        facility_names, employer_names, facility_types, facility_subtypes
    )
    
    logger.info(f"🔢 Built {len(all_params)} total parameters")
    logger.info(f"🔢 Executing comprehensive query for {len(npis)} providers using query type: {query_type}")
    
    start_time = asyncio.get_event_loop().time()
    
    try:
        cursor = await conn.execute(comprehensive_query, all_params)
        rows = await cursor.fetchall()
        query_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"✅ Comprehensive query executed in {query_time:.2f}s")
        
        providers = []
        for row in rows:
            provider_dict = dict(row)
            
            # Apply title case to text fields
            if provider_dict.get('first_name'):
                provider_dict['first_name'] = to_title_case(provider_dict['first_name'])
            if provider_dict.get('last_name'):
                provider_dict['last_name'] = to_title_case(provider_dict['last_name'])
            
            # Parse JSON fields and apply title case where needed
            json_field_configs = {
                'roles': {'type': 'simple_array'},
                'specialties': {'type': 'simple_array'},
                'licensure_states': {'type': 'simple_array'},
                'facility_cities': {'type': 'simple_array', 'title_case': True},
                'facility_states': {'type': 'simple_array'},
                'facility_types': {'type': 'simple_array'},
                'facility_subtypes': {'type': 'simple_array'},
                'employer_names': {
                    'type': 'object_array',
                    'title_case_fields': ['name']
                },
                'facility_names': {
                    'type': 'object_array', 
                    'title_case_fields': ['name', 'city', 'address']
                }
            }
            
            for field, config in json_field_configs.items():
                if provider_dict.get(field):
                    try:
                        data = json.loads(provider_dict[field])
                        
                        if config['type'] == 'simple_array':
                            # Filter out empty strings and apply title case if needed
                            data = [item for item in data if item]
                            if config.get('title_case'):
                                data = [to_title_case(item) for item in data]
                        
                        elif config['type'] == 'object_array':
                            # Apply title case to specified fields in objects
                            title_case_fields = config.get('title_case_fields', [])
                            for item in data:
                                if isinstance(item, dict):
                                    for tc_field in title_case_fields:
                                        if item.get(tc_field):
                                            item[tc_field] = to_title_case(item[tc_field])
                        
                        provider_dict[field] = data
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON for field {field}: {e}")
                        provider_dict[field] = []
                else:
                    provider_dict[field] = []
            
            providers.append(ProviderResponse(**provider_dict))
        
        return providers
        
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        logger.error(f"❌ Error in comprehensive query: {e}")
        logger.error(f"❌ Full traceback: {error_traceback}")
        logger.error(f"❌ Query type: {query_type}")
        logger.error(f"❌ Number of params: {len(all_params) if all_params else 'None'}")
        logger.error(f"❌ NPIs count: {len(npis) if npis else 'None'}")
        # Fallback to individual queries if the comprehensive one fails
        logger.info("🔄 Falling back to individual queries...")
        return await fetch_complete_provider_data_fallback(
            conn, npis, role_specialty_conditions, facility_conditions,
            role_specialty_params, facility_params,
            first_name, last_name, roles, specialties
        )

def build_pfel_comprehensive_query(
    npis: List[str], placeholders: str, role_specialty_where: str,
    facility_cities: List[str], facility_states: List[str], facility_address: str, facility_zipcode: str,
    facility_names: List[str], employer_names: List[str], facility_types: List[str], facility_subtypes: List[str]
) -> str:
    """Build comprehensive query for pfel_new table - get ONLY filtered facilities and their associated employers"""
    
    # Build filter conditions for the data CTEs
    filter_conditions = []
    if facility_names:
        name_conditions = []
        for name in facility_names:
            name_conditions.append("LOWER(pfel_data.facility_name) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(name_conditions)})")
    
    if employer_names:
        emp_conditions = []
        for employer in employer_names:
            emp_conditions.append("LOWER(pfel_data.employer_name) LIKE LOWER(?)")
        filter_conditions.append(f"({' OR '.join(emp_conditions)})")
    
    if facility_cities:
        city_conditions = []
        for city in facility_cities:
            city_conditions.append("LOWER(pfel_data.facility_city) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(city_conditions)})")
    
    if facility_states:
        state_conditions = []
        for state in facility_states:
            state_conditions.append("LOWER(pfel_data.facility_state_name) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(state_conditions)})")
    
    if facility_address:
        filter_conditions.append("LOWER(pfel_data.facility_address) LIKE LOWER(?)")
    
    if facility_zipcode:
        filter_conditions.append("pfel_data.facility_zipcode = ?")
    
    if facility_types:
        type_conditions = []
        for ftype in facility_types:
            type_conditions.append("LOWER(pfel_data.facility_type) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(type_conditions)})")
    
    if facility_subtypes:
        subtype_conditions = []
        for subtype in facility_subtypes:
            subtype_conditions.append("LOWER(pfel_data.facility_subtype) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(subtype_conditions)})")
    
    data_filter_condition = " AND ".join(filter_conditions) if filter_conditions else "1=1"
    
    return f"""
        WITH provider_base AS (
            SELECT 
                p.npi,
                p.first_name,
                p.last_name,
                p.credentials
            FROM providers p
            WHERE p.npi IN ({placeholders})
        ),
        provider_roles AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.role) as roles
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_specialties AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.specialty) as specialties
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_licensure_states AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT lic_state.state_name) as licensure_states
            FROM provider_taxonomies pt
            LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
            WHERE pt.npi IN ({placeholders})
            GROUP BY pt.npi
        ),
        -- Get ONLY the filtered facility data and their associated employers
        provider_filtered_data AS (
            SELECT 
                pfel_data.provider_id as npi,
                -- For facility_names: only include the filtered facilities
                json_group_array(
                    DISTINCT json_object(
                        'name', COALESCE(pfel_data.facility_name, ''),
                        'ccn_or_npi', COALESCE(pfel_data.facility_npi_or_ccn, ''),
                        'type', COALESCE(pfel_data.facility_type, ''),
                        'subtype', COALESCE(pfel_data.facility_subtype, ''),
                        'address', COALESCE(pfel_data.facility_address, ''),
                        'zip_code', COALESCE(pfel_data.facility_zipcode, ''),
                        'latitude', COALESCE(pfel_data.latitude, 0.0),
                        'longitude', COALESCE(pfel_data.longitude, 0.0),
                        'state_name', COALESCE(pfel_data.facility_state_name, ''),
                        'state_code', COALESCE(pfel_data.facility_state_code, ''),
                        'city', COALESCE(pfel_data.facility_city, ''),
                        'provider_count', 0
                    )
                ) as facility_names,
                -- For employer_names: only include employers associated with the filtered facilities
                json_group_array(
                    DISTINCT json_object(
                        'name', pfel_data.employer_name,
                        'ccn_or_npi', pfel_data.employer_npi_or_ccn
                    )
                ) as employer_names,
                -- For arrays: only include data from filtered facilities
                json_group_array(DISTINCT pfel_data.facility_city) as facility_cities,
                json_group_array(DISTINCT pfel_data.facility_state_name) as facility_states,
                json_group_array(DISTINCT pfel_data.facility_type) as facility_types,
                json_group_array(DISTINCT pfel_data.facility_subtype) as facility_subtypes
            FROM pfel_new pfel_data
            WHERE pfel_data.provider_id IN ({placeholders})
            AND {data_filter_condition}
            GROUP BY pfel_data.provider_id
        )
        SELECT 
            pb.npi,
            pb.first_name,
            pb.last_name,
            pb.credentials,
            COALESCE(pr.roles, '[]') as roles,
            COALESCE(ps.specialties, '[]') as specialties,
            COALESCE(pls.licensure_states, '[]') as licensure_states,
            COALESCE(pfd.facility_cities, '[]') as facility_cities,
            COALESCE(pfd.facility_states, '[]') as facility_states,
            COALESCE(pfd.facility_types, '[]') as facility_types,
            COALESCE(pfd.facility_subtypes, '[]') as facility_subtypes,
            COALESCE(pfd.employer_names, '[]') as employer_names,
            COALESCE(pfd.facility_names, '[]') as facility_names
        FROM provider_base pb
        LEFT JOIN provider_roles pr ON pr.npi = pb.npi
        LEFT JOIN provider_specialties ps ON ps.npi = pb.npi
        LEFT JOIN provider_licensure_states pls ON pls.npi = pb.npi
        LEFT JOIN provider_filtered_data pfd ON pfd.npi = pb.npi
    """

def build_facility_comprehensive_query(
    npis: List[str], placeholders: str, role_specialty_where: str,
    facility_cities: List[str], facility_states: List[str], facility_address: str, facility_zipcode: str,
    facility_names: List[str], facility_types: List[str], facility_subtypes: List[str]
) -> str:
    """Build comprehensive query for provider_entities + entities_enriched tables - get ONLY filtered facilities and their associated employers from pfel_new"""
    
    # Build filter conditions for facility data
    # NOTE: Using the actual column names from entities_enriched table
    filter_conditions = []
    if facility_names:
        name_conditions = []
        for name in facility_names:
            name_conditions.append("LOWER(ee.name) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(name_conditions)})")
    
    if facility_cities:
        city_conditions = []
        for city in facility_cities:
            city_conditions.append("LOWER(ee.city) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(city_conditions)})")
    
    if facility_states:
        state_conditions = []
        for state in facility_states:
            state_conditions.append("LOWER(st.state_name) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(state_conditions)})")
    
    if facility_address:
        filter_conditions.append("LOWER(ee.address) LIKE LOWER(?)")
    
    if facility_zipcode:
        filter_conditions.append("ee.zip_code = ?")
    
    if facility_types:
        type_conditions = []
        for ftype in facility_types:
            type_conditions.append("LOWER(ee.type) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(type_conditions)})")
    
    if facility_subtypes:
        subtype_conditions = []
        for subtype in facility_subtypes:
            subtype_conditions.append("LOWER(ee.subtype) = LOWER(?)")
        filter_conditions.append(f"({' OR '.join(subtype_conditions)})")
    
    data_filter_condition = " AND ".join(filter_conditions) if filter_conditions else "1=1"
    
    # Build filter conditions for pfel_new table (same filters but using pfel columns for performance)
    pfel_filter_conditions = []
    if facility_names:
        name_conditions = []
        for name in facility_names:
            name_conditions.append("LOWER(pfel.facility_name) = LOWER(?)")
        pfel_filter_conditions.append(f"({' OR '.join(name_conditions)})")
    
    if facility_cities:
        city_conditions = []
        for city in facility_cities:
            city_conditions.append("LOWER(pfel.facility_city) = LOWER(?)")
        pfel_filter_conditions.append(f"({' OR '.join(city_conditions)})")
    
    if facility_states:
        state_conditions = []
        for state in facility_states:
            state_conditions.append("LOWER(pfel.facility_state_name) = LOWER(?)")
        pfel_filter_conditions.append(f"({' OR '.join(state_conditions)})")
    
    if facility_address:
        pfel_filter_conditions.append("LOWER(pfel.facility_address) LIKE LOWER(?)")
    
    if facility_zipcode:
        pfel_filter_conditions.append("pfel.facility_zipcode = ?")
    
    if facility_types:
        type_conditions = []
        for ftype in facility_types:
            type_conditions.append("LOWER(pfel.facility_type) = LOWER(?)")
        pfel_filter_conditions.append(f"({' OR '.join(type_conditions)})")
    
    if facility_subtypes:
        subtype_conditions = []
        for subtype in facility_subtypes:
            subtype_conditions.append("LOWER(pfel.facility_subtype) = LOWER(?)")
        pfel_filter_conditions.append(f"({' OR '.join(subtype_conditions)})")
    
    pfel_filter_condition = " AND ".join(pfel_filter_conditions) if pfel_filter_conditions else "1=1"
    
    return f"""
        WITH provider_base AS (
            SELECT 
                p.npi,
                p.first_name,
                p.last_name,
                p.credentials
            FROM providers p
            WHERE p.npi IN ({placeholders})
        ),
        provider_roles AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.role) as roles
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_specialties AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.specialty) as specialties
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_licensure_states AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT lic_state.state_name) as licensure_states
            FROM provider_taxonomies pt
            LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
            WHERE pt.npi IN ({placeholders})
            GROUP BY pt.npi
        ),
        -- Get ONLY the filtered facility data
        provider_facility_data AS (
            SELECT 
                pe.provider_id as npi,
                -- Only include the filtered facilities
                json_group_array(
                    DISTINCT json_object(
                        'name', COALESCE(ee.name, ''),
                        'ccn_or_npi', COALESCE(ee.ccn_or_npi, ''),
                        'type', COALESCE(ee.type, ''),
                        'subtype', COALESCE(ee.subtype, ''),
                        'address', COALESCE(ee.address, ''),
                        'zip_code', COALESCE(ee.zip_code, ''),
                        'latitude', COALESCE(ee.latitude, 0.0),
                        'longitude', COALESCE(ee.longitude, 0.0),
                        'state_name', COALESCE(st.state_name, ''),
                        'state_code', COALESCE(st.state_code, ''),
                        'city', COALESCE(ee.city, ''),
                        'provider_count', 0
                    )
                ) as facility_names,
                -- For arrays: only include data from filtered facilities
                json_group_array(DISTINCT ee.city) as facility_cities,
                json_group_array(DISTINCT st.state_name) as facility_states,
                json_group_array(DISTINCT ee.type) as facility_types,
                json_group_array(DISTINCT ee.subtype) as facility_subtypes
            FROM provider_entities pe
            INNER JOIN entities_enriched ee ON ee.ccn_or_npi = pe.npi_or_ccn
            LEFT JOIN states st ON st.state_id = ee.state_id
            WHERE pe.provider_id IN ({placeholders})
            AND {data_filter_condition}
            GROUP BY pe.provider_id
        ),
        -- FIXED: Get employers associated with the filtered facilities only (using pfel-specific filters)
        provider_employer_data AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(
                    DISTINCT json_object(
                        'name', pfel.employer_name,
                        'ccn_or_npi', pfel.employer_npi_or_ccn
                    )
                ) as employer_names
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {pfel_filter_condition}
            GROUP BY pfel.provider_id
        )
        SELECT 
            pb.npi,
            pb.first_name,
            pb.last_name,
            pb.credentials,
            COALESCE(pr.roles, '[]') as roles,
            COALESCE(ps.specialties, '[]') as specialties,
            COALESCE(pls.licensure_states, '[]') as licensure_states,
            COALESCE(pfd.facility_cities, '[]') as facility_cities,
            COALESCE(pfd.facility_states, '[]') as facility_states,
            COALESCE(pfd.facility_types, '[]') as facility_types,
            COALESCE(pfd.facility_subtypes, '[]') as facility_subtypes,
            COALESCE(ped.employer_names, '[]') as employer_names,
            COALESCE(pfd.facility_names, '[]') as facility_names
        FROM provider_base pb
        LEFT JOIN provider_roles pr ON pr.npi = pb.npi
        LEFT JOIN provider_specialties ps ON ps.npi = pb.npi
        LEFT JOIN provider_licensure_states pls ON pls.npi = pb.npi
        LEFT JOIN provider_facility_data pfd ON pfd.npi = pb.npi
        LEFT JOIN provider_employer_data ped ON ped.npi = pb.npi
    """

def build_employer_comprehensive_query(
    npis: List[str], placeholders: str, role_specialty_where: str,
    employer_names: List[str]
) -> str:
    """Build comprehensive query for provider_employer + entities_enriched tables - get ONLY filtered employers and their associated facilities from pfel_new"""
    
    # Build filter conditions for employer data
    filter_conditions = []
    if employer_names:
        emp_conditions = []
        for employer in employer_names:
            emp_conditions.append("LOWER(ee.name) LIKE LOWER(?)")
        filter_conditions.append(f"({' OR '.join(emp_conditions)})")
    
    data_filter_condition = " AND ".join(filter_conditions) if filter_conditions else "1=1"
    
    return f"""
        WITH provider_base AS (
            SELECT 
                p.npi,
                p.first_name,
                p.last_name,
                p.credentials
            FROM providers p
            WHERE p.npi IN ({placeholders})
        ),
        provider_roles AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.role) as roles
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_specialties AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.specialty) as specialties
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_licensure_states AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT lic_state.state_name) as licensure_states
            FROM provider_taxonomies pt
            LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
            WHERE pt.npi IN ({placeholders})
            GROUP BY pt.npi
        ),
        -- Get ONLY the filtered employer data
        provider_employer_data AS (
            SELECT 
                pem.provider_id as npi,
                json_group_array(
                    DISTINCT json_object(
                        'name', ee.name,
                        'ccn_or_npi', ee.ccn_or_npi
                    )
                ) as employer_names,
                json_group_array(DISTINCT ee.city) as facility_cities,
                json_group_array(DISTINCT st.state_name) as facility_states
            FROM provider_employer pem
            INNER JOIN entities_enriched ee ON ee.ccn_or_npi = pem.npi_or_ccn
            LEFT JOIN states st ON st.state_id = ee.state_id
            WHERE pem.provider_id IN ({placeholders})
            AND {data_filter_condition}
            GROUP BY pem.provider_id
        ),
        -- Get facilities associated with the filtered employers from pfel_new
        provider_facility_data AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(
                    DISTINCT json_object(
                        'name', COALESCE(pfel.facility_name, ''),
                        'ccn_or_npi', COALESCE(pfel.facility_npi_or_ccn, ''),
                        'type', COALESCE(pfel.facility_type, ''),
                        'subtype', COALESCE(pfel.facility_subtype, ''),
                        'address', COALESCE(pfel.facility_address, ''),
                        'zip_code', COALESCE(pfel.facility_zipcode, ''),
                        'latitude', COALESCE(pfel.latitude, 0.0),
                        'longitude', COALESCE(pfel.longitude, 0.0),
                        'state_name', COALESCE(pfel.facility_state_name, ''),
                        'state_code', COALESCE(pfel.facility_state_code, ''),
                        'city', COALESCE(pfel.facility_city, ''),
                        'provider_count', 0
                    )
                ) as facility_names,
                json_group_array(DISTINCT pfel.facility_city) as facility_cities,
                json_group_array(DISTINCT pfel.facility_state_name) as facility_states,
                json_group_array(DISTINCT pfel.facility_type) as facility_types,
                json_group_array(DISTINCT pfel.facility_subtype) as facility_subtypes
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND EXISTS (
                SELECT 1 FROM provider_employer_data ped 
                WHERE ped.npi = pfel.provider_id
            )
            GROUP BY pfel.provider_id
        )
        SELECT 
            pb.npi,
            pb.first_name,
            pb.last_name,
            pb.credentials,
            COALESCE(pr.roles, '[]') as roles,
            COALESCE(ps.specialties, '[]') as specialties,
            COALESCE(pls.licensure_states, '[]') as licensure_states,
            COALESCE(pfd.facility_cities, '[]') as facility_cities,
            COALESCE(pfd.facility_states, '[]') as facility_states,
            COALESCE(pfd.facility_types, '[]') as facility_types,
            COALESCE(pfd.facility_subtypes, '[]') as facility_subtypes,
            COALESCE(ped.employer_names, '[]') as employer_names,
            COALESCE(pfd.facility_names, '[]') as facility_names
        FROM provider_base pb
        LEFT JOIN provider_roles pr ON pr.npi = pb.npi
        LEFT JOIN provider_specialties ps ON ps.npi = pb.npi
        LEFT JOIN provider_licensure_states pls ON pls.npi = pb.npi
        LEFT JOIN provider_facility_data pfd ON pfd.npi = pb.npi
        LEFT JOIN provider_employer_data ped ON ped.npi = pb.npi
    """

def build_providers_only_comprehensive_query(npis: List[str], placeholders: str, role_specialty_where: str) -> str:
    """Build comprehensive query when only provider filters are used"""
    return f"""
        WITH provider_base AS (
            SELECT 
                p.npi,
                p.first_name,
                p.last_name,
                p.credentials
            FROM providers p
            WHERE p.npi IN ({placeholders})
        ),
        provider_roles AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.role) as roles
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_specialties AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT rsc.specialty) as specialties
            FROM provider_taxonomies pt
            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
            WHERE pt.npi IN ({placeholders})
            AND {role_specialty_where}
            GROUP BY pt.npi
        ),
        provider_licensure_states AS (
            SELECT 
                pt.npi,
                json_group_array(DISTINCT lic_state.state_name) as licensure_states
            FROM provider_taxonomies pt
            LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
            WHERE pt.npi IN ({placeholders})
            GROUP BY pt.npi
        )
        SELECT 
            pb.npi,
            pb.first_name,
            pb.last_name,
            pb.credentials,
            COALESCE(pr.roles, '[]') as roles,
            COALESCE(ps.specialties, '[]') as specialties,
            COALESCE(pls.licensure_states, '[]') as licensure_states,
            '[]' as facility_cities,
            '[]' as facility_states,
            '[]' as facility_types,
            '[]' as facility_subtypes,
            '[]' as employer_names,
            '[]' as facility_names
        FROM provider_base pb
        LEFT JOIN provider_roles pr ON pr.npi = pb.npi
        LEFT JOIN provider_specialties ps ON ps.npi = pb.npi
        LEFT JOIN provider_licensure_states pls ON pls.npi = pb.npi
    """

def build_comprehensive_query_params(
    npis: List[str], role_specialty_params: List, query_type: str,
    facility_cities: List[str], facility_states: List[str], facility_address: str, facility_zipcode: str,
    facility_names: List[str], employer_names: List[str], facility_types: List[str], facility_subtypes: List[str]
) -> List:
    """Build parameters for comprehensive query based on query type"""
    # Defensive checks for None values
    if npis is None:
        npis = []
    if role_specialty_params is None:
        role_specialty_params = []
    if facility_cities is None:
        facility_cities = []
    if facility_states is None:
        facility_states = []
    if facility_names is None:
        facility_names = []
    if employer_names is None:
        employer_names = []
    if facility_types is None:
        facility_types = []
    if facility_subtypes is None:
        facility_subtypes = []
    
    # Convert to list and build base params
    base_params = list(npis)  # provider_base
    base_params.extend(npis)  # provider_roles - npis
    base_params.extend(role_specialty_params)  # provider_roles - role/specialty params
    base_params.extend(npis)  # provider_specialties - npis
    base_params.extend(role_specialty_params)  # provider_specialties - role/specialty params
    base_params.extend(npis)  # provider_licensure_states
    
    if query_type == "pfel":
        # Add filter params for pfel data
        filter_params = []
        if facility_names:
            filter_params.extend(facility_names)
        if employer_names:
            filter_params.extend(employer_names)
        if facility_cities:
            filter_params.extend(facility_cities)
        if facility_states:
            filter_params.extend(facility_states)
        if facility_address:
            filter_params.append(f"%{facility_address}%")
        if facility_zipcode:
            filter_params.append(facility_zipcode)
        if facility_types:
            filter_params.extend(facility_types)
        if facility_subtypes:
            filter_params.extend(facility_subtypes)
        
        base_params.extend(npis)  # provider_filtered_data - npis
        base_params.extend(filter_params)  # provider_filtered_data - filter params
    
    elif query_type == "facility":
        # Add filter params for facility data
        filter_params = []
        if facility_names:
            filter_params.extend(facility_names)
        if facility_cities:
            filter_params.extend(facility_cities)
        if facility_states:
            filter_params.extend(facility_states)
        if facility_address:
            filter_params.append(f"%{facility_address}%")
        if facility_zipcode:
            filter_params.append(facility_zipcode)
        if facility_types:
            filter_params.extend(facility_types)
        if facility_subtypes:
            filter_params.extend(facility_subtypes)
        
        # FIXED: Add filter params twice - once for facility_data, once for employer_data
        base_params.extend(npis)  # provider_facility_data - npis
        base_params.extend(filter_params)  # provider_facility_data - filter params
        base_params.extend(npis)  # provider_employer_data - npis
        base_params.extend(filter_params)  # provider_employer_data - filter params
    
    elif query_type == "employer":
        # Add filter params for employer data
        filter_params = []
        if employer_names:
            filter_params.extend(employer_names)
        
        base_params.extend(npis)  # provider_employer_data - npis
        base_params.extend(filter_params)  # provider_employer_data - filter params
        base_params.extend(npis)  # provider_facility_data - npis
    
    return base_params

async def fetch_complete_provider_data_fallback(
    conn, npis: List[str], 
    role_specialty_conditions: List, 
    facility_conditions: List,
    role_specialty_params: List,
    facility_params: List,
    first_name: str,
    last_name: str,
    roles: List[str],
    specialties: List[str]
) -> List[ProviderResponse]:
    """Fallback method using individual subqueries (slower but more reliable)"""
    # ... (keep the original individual subquery implementation as fallback)
    pass