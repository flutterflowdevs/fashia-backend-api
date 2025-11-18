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

def build_facility_conditions(
    facility_cities: List[str],
    facility_states: List[str],
    facility_address: str,
    facility_zipcode: str,
    facility_names: List[str],
    employer_names: List[str],
    facility_types: List[str],
    facility_subtypes: List[str]
) -> tuple:
    """Build facility-related conditions"""
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
            name_conditions.append("LOWER(pfel.facility_name) LIKE LOWER(?)")
            params.append(f"%{name}%")
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
            
            facility_conditions, facility_params = build_facility_conditions(
                facility_cities, facility_states, facility_address, facility_zipcode,
                facility_names, employer_names, facility_types, facility_subtypes
            )
            
            # Determine join type for pfel table
            has_facility_filter = bool(facility_conditions)
            pfel_join_type = "INNER" if has_facility_filter else "LEFT"
            
            # Add facility conditions to main query if needed
            if facility_conditions:
                base_conditions.extend(facility_conditions)
                base_params.extend(facility_params)
            
            # Build WHERE clause
            where_clause = "WHERE " + " AND ".join(base_conditions) if base_conditions else ""
            logger.info(f"📝 Final WHERE clause: {where_clause}")
            logger.info(f"🔢 Base params count: {len(base_params)}")
            
            # Step 1: Get total count using CTE for better performance
            count_query = f"""
                WITH filtered_providers AS (
                    SELECT DISTINCT p.npi
                    FROM providers p
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                    LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    {pfel_join_type} JOIN pfel_new pfel ON pfel.provider_id = p.npi
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
            order_by_clause = build_order_by_clause(sort_by, sort_order, role_specialty_conditions, facility_conditions)
            
            # Get sorted NPIs using CTE
            sorted_npis_query = f"""
                WITH filtered_providers AS (
                    SELECT DISTINCT p.npi
                    FROM providers p
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                    LEFT JOIN states lic_state ON lic_state.state_id = pt.license_state_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    {pfel_join_type} JOIN pfel_new pfel ON pfel.provider_id = p.npi
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
                first_name, last_name, roles, specialties
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

def build_order_by_clause(sort_by: str, sort_order: str, role_specialty_conditions: List, facility_conditions: List) -> str:
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
                    "facility_type", "facility_subtype"]:
        # For facility-related sorting
        facility_where = " AND ".join(facility_conditions) if facility_conditions else "1=1"
        field_map = {
            "facility_city": "facility_city",
            "facility_state": "facility_state_name", 
            "facility_name": "facility_name",
            "employer_name": "employer_name",
            "facility_type": "facility_type",
            "facility_subtype": "facility_subtype"
        }
        field = field_map.get(sort_by, "facility_name")
        
        return f"""
            (SELECT pfel.{field} FROM pfel_new pfel
             WHERE pfel.provider_id = p.npi AND {facility_where}
             ORDER BY pfel.{field} {sort_order} LIMIT 1) {sort_order},
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
    specialties: List[str]
) -> List[ProviderResponse]:
    """Fetch complete provider data with all relationships using optimized CTEs"""
    placeholders = ','.join(['?' for _ in npis])
    
    # Build WHERE conditions for subqueries
    role_specialty_where = " AND ".join(role_specialty_conditions) if role_specialty_conditions else "1=1"
    facility_where = " AND ".join(facility_conditions) if facility_conditions else "1=1"
    
    # Build provider name conditions for provider_count
    provider_name_conditions = []
    provider_name_params = []
    
    if first_name:
        provider_name_conditions.append("LOWER(prov_count.first_name) = LOWER(?)")
        provider_name_params.append(first_name)
    
    if last_name:
        provider_name_conditions.append("LOWER(prov_count.last_name) = LOWER(?)")
        provider_name_params.append(last_name)
    
    provider_name_where = " AND ".join(provider_name_conditions) if provider_name_conditions else "1=1"
    
    # Build role/specialty conditions for provider_count
    role_specialty_count_conditions = []
    role_specialty_count_params = []
    
    if roles:
        for role in roles:
            role_specialty_count_conditions.append("LOWER(rsc_count.role) = LOWER(?)")
            role_specialty_count_params.append(role)
    
    if specialties:
        for specialty in specialties:
            role_specialty_count_conditions.append("LOWER(rsc_count.specialty) = LOWER(?)")
            role_specialty_count_params.append(specialty)
    
    role_specialty_count_where = " AND ".join(role_specialty_count_conditions) if role_specialty_count_conditions else "1=1"
    
    logger.info("🚀 Using optimized CTE approach for better performance...")
    
    # Single comprehensive query using CTEs with REAL provider_count
    comprehensive_query = f"""
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
        provider_facility_cities AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(DISTINCT pfel.facility_city) as facility_cities
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
            AND pfel.facility_city IS NOT NULL
            AND pfel.facility_city != ''
            GROUP BY pfel.provider_id
        ),
        provider_facility_states AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(DISTINCT pfel.facility_state_name) as facility_states
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
            AND pfel.facility_state_name IS NOT NULL
            AND pfel.facility_state_name != ''
            GROUP BY pfel.provider_id
        ),
        provider_facility_types AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(DISTINCT pfel.facility_type) as facility_types
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
            AND pfel.facility_type IS NOT NULL
            AND pfel.facility_type != ''
            GROUP BY pfel.provider_id
        ),
        provider_facility_subtypes AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(DISTINCT pfel.facility_subtype) as facility_subtypes
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
            AND pfel.facility_subtype IS NOT NULL
            AND pfel.facility_subtype != ''
            GROUP BY pfel.provider_id
        ),
        provider_employer_names AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(
                    json_object(
                        'name', pfel.employer_name,
                        'ccn_or_npi', pfel.employer_npi_or_ccn
                    )
                ) as employer_names
            FROM pfel_new pfel
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
            AND pfel.employer_name IS NOT NULL
            AND pfel.employer_name != ''
            GROUP BY pfel.provider_id
        ),
        -- Facility data with REAL provider_count
        facility_data AS (
            SELECT 
                pe.provider_id as npi,
                pe.npi_or_ccn,
                COUNT(DISTINCT prov_count.npi) as real_provider_count
            FROM provider_entities pe
            INNER JOIN pfel_new pfel_count ON pfel_count.facility_npi_or_ccn = pe.npi_or_ccn
            INNER JOIN providers prov_count ON prov_count.npi = pfel_count.provider_id
            INNER JOIN provider_taxonomies pt_count ON pt_count.npi = prov_count.npi
            INNER JOIN roles_specialties_classification rsc_count ON rsc_count.nucc_code = pt_count.nucc_code
            WHERE pe.provider_id IN ({placeholders})
            AND {facility_where}
            AND {provider_name_where}
            AND {role_specialty_count_where}
            GROUP BY pfel.provider_id, pfel.facility_npi_or_ccn
        ),
        provider_facility_names AS (
            SELECT 
                pfel.provider_id as npi,
                json_group_array(
                    json_object(
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
                        'provider_count', COALESCE(fd.real_provider_count, 0)
                    )
                ) as facility_names
            FROM pfel_new pfel
            LEFT JOIN facility_data fd ON fd.npi = pfel.provider_id AND fd.facility_npi_or_ccn = pfel.facility_npi_or_ccn
            WHERE pfel.provider_id IN ({placeholders})
            AND {facility_where}
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
            COALESCE(pfc.facility_cities, '[]') as facility_cities,
            COALESCE(pfs.facility_states, '[]') as facility_states,
            COALESCE(pft.facility_types, '[]') as facility_types,
            COALESCE(pfst.facility_subtypes, '[]') as facility_subtypes,
            COALESCE(pen.employer_names, '[]') as employer_names,
            COALESCE(pfn.facility_names, '[]') as facility_names
        FROM provider_base pb
        LEFT JOIN provider_roles pr ON pr.npi = pb.npi
        LEFT JOIN provider_specialties ps ON ps.npi = pb.npi
        LEFT JOIN provider_licensure_states pls ON pls.npi = pb.npi
        LEFT JOIN provider_facility_cities pfc ON pfc.npi = pb.npi
        LEFT JOIN provider_facility_states pfs ON pfs.npi = pb.npi
        LEFT JOIN provider_facility_types pft ON pft.npi = pb.npi
        LEFT JOIN provider_facility_subtypes pfst ON pfst.npi = pb.npi
        LEFT JOIN provider_employer_names pen ON pen.npi = pb.npi
        LEFT JOIN provider_facility_names pfn ON pfn.npi = pb.npi
    """
    
    # Build parameters - we need to repeat npis for each CTE that uses them
    all_params = (
        npis +  # provider_base
        npis + role_specialty_params +  # provider_roles
        npis + role_specialty_params +  # provider_specialties  
        npis +  # provider_licensure_states
        npis + facility_params +  # provider_facility_cities
        npis + facility_params +  # provider_facility_states
        npis + facility_params +  # provider_facility_types
        npis + facility_params +  # provider_facility_subtypes
        npis + facility_params +  # provider_employer_names
        npis + facility_params + provider_name_params + role_specialty_count_params +  # facility_data
        npis + facility_params    # provider_facility_names
    )
    
    logger.info(f"🔢 Executing single comprehensive query for {len(npis)} providers")
    logger.info(f"🔍 Provider count filters - name: {provider_name_where}, roles/specialties: {role_specialty_count_where}")
    
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
        logger.error(f"❌ Error in comprehensive query: {e}")
        # Fallback to individual queries if the comprehensive one fails
        logger.info("🔄 Falling back to individual queries...")
        return await fetch_complete_provider_data_fallback(
            conn, npis, role_specialty_conditions, facility_conditions,
            role_specialty_params, facility_params,
            first_name, last_name, roles, specialties
        )

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
    # This would be your previous implementation
    pass