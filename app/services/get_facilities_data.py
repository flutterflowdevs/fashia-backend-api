import os
import logging
from typing import List, Optional
from fastapi import HTTPException
from app.db.session import get_db_connection
from app.models.facility_model import FacilityResponse, PaginatedEntityResponse
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

async def get_facilities_data(
    name: Optional[str] = "",
    cities: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    address: Optional[str] = "",
    zipcode: Optional[str] = "",
    roles: Optional[List[str]] = None,
    specialties: Optional[List[str]] = None,
    employers: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    subtypes: Optional[List[str]] = None,
    provider_first_name: Optional[str] = "",
    provider_last_name: Optional[str] = "",
    coords: Optional[List[dict]] = None,
    page: int = 1,
    per_page: int = 25,
    sort_by: str = "name",
    sort_order: str = "ASC",
):
    """
    Fetch paginated facilities from local SQLite database.
    Facilities are entities where is_employer = 0.
    """
    # Log function entry and parameters
    logger.info("🔍 get_facilities_data called")
    logger.info(f"📊 Parameters - name: {name}, cities: {cities}, states: {states}, page: {page}, per_page: {per_page}")
    logger.info(f"🔧 Container User Info - UID: {os.getuid()}, GID: {os.getgid()}, User: {os.getenv('USER', 'Unknown')}")
    
    # Handle None values for lists
    cities = cities or []
    states = states or []
    roles = roles or []
    specialties = specialties or []
    employers = employers or []
    types = types or []
    subtypes = subtypes or []
    coords = coords or []
    
    # Validate sort parameters
    valid_sort_fields = ["name", "type", "subtype", "city", "state_name", "address", "zip_code", "role", "specialty", "employer", "provider_count"]
    if sort_by not in valid_sort_fields:
        logger.warning(f"Invalid sort_by: {sort_by}, defaulting to 'name'")
        sort_by = "name"
    
    sort_order = sort_order.upper()
    if sort_order not in ["ASC", "DESC"]:
        logger.warning(f"Invalid sort_order: {sort_order}, defaulting to 'ASC'")
        sort_order = "ASC"
    
    logger.info(f"🎯 Final sort - by: {sort_by}, order: {sort_order}")
    
    try:
        # Log database connection attempt
        logger.info("🔄 Attempting database connection...")
        
        async with get_db_connection() as conn:
            logger.info("✅ Database connection established successfully")
            
            # Log database info
            try:
                cursor = await conn.execute("PRAGMA database_list")
                db_info = await cursor.fetchall()
                for db in db_info:
                    logger.info(f"📁 Database file: {db[2]} (seq: {db[0]}, name: {db[1]})")
            except Exception as e:
                logger.warning(f"Could not get database info: {e}")
            
            # Set query optimizations
            await conn.execute("PRAGMA temp_store = MEMORY")
            await conn.execute("PRAGMA cache_size = -64000")
            logger.debug("Database optimizations applied")
            
            # Build base filters
            entity_params = []
            filters = ["e.is_employer = 0"]
            logger.info("Building query filters...")

            # Name contains search
            if name:
                filters.append("LOWER(e.name) LIKE ?")
                entity_params.append(f"%{name.lower()}%")
                logger.debug(f"Added name filter: {name}")
            
            if cities:
                city_conditions = []
                for city in cities:
                    city_conditions.append("LOWER(e.city) = ?")
                    entity_params.append(city.lower())
                filters.append(f"({' OR '.join(city_conditions)})")
                logger.debug(f"Added city filters: {cities}")
            
            if states:
                state_conditions = []
                for state in states:
                    state_conditions.append("LOWER(s.state_name) = ?")
                    entity_params.append(state.lower())
                filters.append(f"({' OR '.join(state_conditions)})")
                logger.debug(f"Added state filters: {states}")
            
            if address:
                filters.append("LOWER(e.address) LIKE ?")
                entity_params.append(f"%{address.lower()}%")
                logger.debug(f"Added address filter: {address}")
            
            if zipcode:
                filters.append("e.zip_code = ?")
                entity_params.append(zipcode)
                logger.debug(f"Added zipcode filter: {zipcode}")
            
            if types:
                type_conditions = []
                for facility_type in types:
                    type_conditions.append("LOWER(e.type) = ?")
                    entity_params.append(facility_type.lower())
                filters.append(f"({' OR '.join(type_conditions)})")
                logger.debug(f"Added type filters: {types}")
            
            if subtypes:
                subtype_conditions = []
                for subtype in subtypes:
                    subtype_conditions.append("LOWER(e.subtype) = ?")
                    entity_params.append(subtype.lower())
                filters.append(f"({' OR '.join(subtype_conditions)})")
                logger.debug(f"Added subtype filters: {subtypes}")

            # Bounding box coordinates filter
            if coords and len(coords) >= 2:
                lats = [coord.get('lat') for coord in coords if coord.get('lat') is not None]
                lngs = [coord.get('lng') for coord in coords if coord.get('lng') is not None]
                
                if lats and lngs:
                    lat_min, lat_max = min(lats), max(lats)
                    lng_min, lng_max = min(lngs), max(lngs)
                    
                    filters.append("e.latitude BETWEEN ? AND ?")
                    entity_params.extend([lat_min, lat_max])
                    filters.append("e.longitude BETWEEN ? AND ?")
                    entity_params.extend([lng_min, lng_max])
                    logger.debug(f"Added coordinate filter: lat({lat_min}-{lat_max}), lng({lng_min}-{lng_max})")

            # Build provider filter conditions and parameters separately
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
            
            # Determine if we need provider_entities join for employer query
            needs_provider_join = bool(provider_first_name or provider_last_name)
            logger.debug(f"Needs provider join: {needs_provider_join}")
            
            # Build COMBINED filter for roles, specialties, and provider_count subqueries
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
                    combined_conditions.append("LOWER(rsc.specialty) = LOWER(?)")
                    combined_params.append(specialty)
                logger.debug(f"Added specialty filters: {specialties}")
            
            # Combined WHERE clause for roles, specialties, and provider_count subqueries
            combined_where_clause = " AND ".join(combined_conditions) if combined_conditions else "1=1"
            
            # For employer subquery, use the combined filter PLUS employer name filter
            employer_combined_conditions = combined_conditions.copy()
            employer_combined_params = combined_params.copy()
            
            if employers:
                emp_conditions = []
                for employer in employers:
                    emp_conditions.append("LOWER(emp.name) LIKE ?")
                    employer_combined_params.append(f"%{employer.lower()}%")
                employer_combined_conditions.append(f"({' OR '.join(emp_conditions)})")
                logger.debug(f"Added employer filters: {employers}")
            
            employer_where_clause = " AND ".join(employer_combined_conditions) if employer_combined_conditions else "1=1"
            
            # Combine all provider-related conditions for the main filter
            if combined_conditions:
                combined_where = " AND ".join(combined_conditions)
                filters.append(f"""EXISTS (
                    SELECT 1 FROM provider_entities pe
                    INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    WHERE pe.npi_or_ccn = e.ccn_or_npi
                    AND ({combined_where})
                )""")
                entity_params.extend(combined_params)
            
            # Employer filters
            if employers:
                filters.append(f"""EXISTS (
                    SELECT 1 FROM provider_facility_employer_linked pfel
                    INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                    INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                    AND ({employer_where_clause})
                )""")
                entity_params.extend(employer_combined_params)

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            logger.info(f"📝 Final WHERE clause: {where_clause}")
            logger.info(f"🔢 Total parameters: {len(entity_params)}")

            # Step 1: Get total count
            count_query = f"""
                SELECT COUNT(DISTINCT e.ccn_or_npi) 
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
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
                return PaginatedEntityResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=0,
                    total_pages=0,
                )

            # Step 2: Build the main query with proper sorting before pagination
            offset = (page - 1) * per_page
            
            # Build the actual WHERE clauses for subqueries (not parameterized)
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

            # Build actual WHERE clauses for subqueries
            actual_combined_where = build_actual_where_clause(combined_where_clause, combined_params)
            actual_employer_where = build_actual_where_clause(employer_where_clause, employer_combined_params)

            # NEW: Build proper sorting logic based on get_paginated_providers approach
            if sort_by in ["role", "specialty", "employer"]:
                # For array-based fields, we need to get the first (lowest) value for sorting
                if sort_by == "role":
                    sort_subquery = f"""
                        SELECT rsc.role
                        FROM provider_entities pe
                        INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                        INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                        WHERE pe.npi_or_ccn = e.ccn_or_npi
                        AND {actual_combined_where}
                        ORDER BY rsc.role {sort_order}
                        LIMIT 1
                    """
                elif sort_by == "specialty":
                    sort_subquery = f"""
                        SELECT rsc.specialty
                        FROM provider_entities pe
                        INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                        INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                        WHERE pe.npi_or_ccn = e.ccn_or_ccn
                        AND {actual_combined_where}
                        ORDER BY rsc.specialty {sort_order}
                        LIMIT 1
                    """
                elif sort_by == "employer":
                    if needs_provider_join:
                        sort_subquery = f"""
                            SELECT emp.name
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                            INNER JOIN provider_entities pe ON pe.provider_id = pfel.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                            AND {actual_employer_where}
                            ORDER BY emp.name {sort_order}
                            LIMIT 1
                        """
                    else:
                        sort_subquery = f"""
                            SELECT emp.name
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                            INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                            AND {actual_employer_where}
                            ORDER BY emp.name {sort_order}
                            LIMIT 1
                        """
                
                # Use the same nulls-last pattern as get_paginated_providers
                order_by_clause = f"""
                    CASE WHEN ({sort_subquery}) IS NULL OR ({sort_subquery}) = '' THEN 1 ELSE 0 END,
                    ({sort_subquery}) {sort_order},
                    e.name ASC
                """
            elif sort_by == "provider_count":
                sort_subquery = f"""
                    SELECT COUNT(DISTINCT pe.provider_id)
                    FROM provider_entities pe
                    INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    WHERE pe.npi_or_ccn = e.ccn_or_npi
                    AND {actual_combined_where}
                """
                order_by_clause = f"""
                    CASE WHEN ({sort_subquery}) IS NULL OR ({sort_subquery}) = 0 THEN 1 ELSE 0 END,
                    ({sort_subquery}) {sort_order},
                    e.name ASC
                """
            else:
                # For simple fields
                simple_field_map = {
                    "name": "e.name",
                    "type": "e.type",
                    "subtype": "e.subtype",
                    "city": "e.city",
                    "state_name": "s.state_name",
                    "address": "e.address",
                    "zip_code": "e.zip_code"
                }
                field = simple_field_map.get(sort_by, "e.name")
                order_by_clause = f"{field} {sort_order}"

            # FIXED: Apply sorting to the entire dataset first, then paginate
            # We'll use a subquery to get the sorted CCNs first
            sorted_ccns_query = f"""
                SELECT e.ccn_or_npi
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
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

            logger.info(f"✅ Sorted CCNs query executed in {ccn_query_time:.2f}s, found {len(ccns)} facilities")

            if not ccns:
                logger.info("❌ No results found after pagination")
                return PaginatedEntityResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=total_count,
                    total_pages=total_pages,
                )

            # Now fetch the full entity data for the sorted CCNs
            placeholders = ','.join(['?' for _ in ccns])
            main_query = f"""
                SELECT
                    e.name,
                    e.type,
                    e.subtype,
                    e.address,
                    e.city,
                    e.zip_code,
                    e.ccn_or_npi,
                    s.state_name AS state,
                    s.state_code AS state_code,
                    e.latitude,
                    e.longitude
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
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

            # Convert rows to basic entities
            basic_entities = []
            for row in rows_sorted:
                row_dict = dict(row)
                if row_dict.get('name'):
                    row_dict['name'] = to_title_case(row_dict['name'])
                if row_dict.get('city'):
                    row_dict['city'] = to_title_case(row_dict['city'])    
                if row_dict.get('address'):
                    row_dict['address'] = to_title_case(row_dict['address'])
                basic_entities.append(row_dict)

            # OPTIMIZATION: Execute subqueries in parallel for each facility
            entities = []
            logger.info(f"🔄 Starting subquery processing for {len(basic_entities)} entities...")
            
            # Build consistent ordering for display in JSON arrays - ALWAYS sort alphabetically for display
            role_display_order = "ORDER BY rsc.role ASC"
            specialty_display_order = "ORDER BY rsc.specialty ASC"
            employer_display_order = "ORDER BY emp.name ASC"
            
            for i, entity in enumerate(basic_entities):
                ccn_or_npi = entity['ccn_or_npi']
                
                # Build employers query conditionally
                if needs_provider_join:
                    employers_query = f"""
                        SELECT json_group_array(json_object('name', emp.name, 'ccn_or_npi', emp.ccn_or_npi))
                        FROM (
                            SELECT DISTINCT emp.name, emp.ccn_or_npi
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                            INNER JOIN provider_entities pe ON pe.provider_id = pfel.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pfel.facility_npi_or_ccn = ?
                            AND {actual_employer_where}
                            {employer_display_order}
                        ) emp
                        """
                else:
                    employers_query = f"""
                        SELECT json_group_array(json_object('name', emp.name, 'ccn_or_npi', emp.ccn_or_npi))
                        FROM (
                            SELECT DISTINCT emp.name, emp.ccn_or_npi
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                            INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pfel.facility_npi_or_ccn = ?
                            AND {actual_employer_where}
                            {employer_display_order}
                        ) emp
                        """
                
                # Define subqueries to run in parallel
                subqueries = [
                    # Providers count
                    (
                        f"""
                        SELECT COUNT(DISTINCT pe.provider_id)
                        FROM provider_entities pe
                        INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                        INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                        WHERE pe.npi_or_ccn = ?
                        AND {actual_combined_where}
                        """,
                        [ccn_or_npi],
                        "providers_count"
                    ),
                    # Employers
                    (
                        employers_query,
                        [ccn_or_npi],
                        "employers"
                    ),
                    # Roles - ALWAYS sort alphabetically for display
                    (
                        f"""
                        SELECT json_group_array(COALESCE(role, ''))
                        FROM (
                            SELECT DISTINCT rsc.role
                            FROM provider_entities pe
                            INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pe.npi_or_ccn = ?
                            AND {actual_combined_where}
                            {role_display_order}
                        )
                        """,
                        [ccn_or_npi],
                        "roles"
                    ),
                    # Specialties - ALWAYS sort alphabetically for display
                    (
                        f"""
                        SELECT json_group_array(COALESCE(specialty, ''))
                        FROM (
                            SELECT DISTINCT rsc.specialty
                            FROM provider_entities pe
                            INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pe.npi_or_ccn = ?
                            AND {actual_combined_where}
                            {specialty_display_order}
                        )
                        """,
                        [ccn_or_npi],
                        "specialties"
                    )
                ]
                
                # Execute all subqueries in parallel
                start_subqueries = asyncio.get_event_loop().time()
                tasks = [execute_subquery(conn, query, params, field) for query, params, field in subqueries]
                results = await asyncio.gather(*tasks)
                subquery_time = asyncio.get_event_loop().time() - start_subqueries
                
                # Combine results
                entity_data = entity.copy()
                for field_name, result in results:
                    if field_name == "employers" and result:
                        employers_data = json.loads(result)
                        for employer in employers_data:
                            if employer.get('name'):
                                employer['name'] = to_title_case(employer['name'])
                        entity_data[field_name] = employers_data
                    elif field_name in ["roles", "specialties"] and result:
                        entity_data[field_name] = json.loads(result)
                    else:
                        entity_data[field_name] = result or 0 if field_name == "providers_count" else result or []
                
                entities.append(FacilityResponse(**entity_data))
                
                if (i + 1) % 10 == 0:  # Log progress every 10 entities
                    logger.info(f"📦 Processed {i + 1}/{len(basic_entities)} entities, last batch subqueries took {subquery_time:.2f}s")

            total_pages = (total_count + per_page - 1) // per_page

            total_processing_time = asyncio.get_event_loop().time() - count_start
            logger.info(f"🎉 Query completed successfully in {total_processing_time:.2f}s")
            logger.info(f"📊 Final result: {len(entities)} entities, {total_count} total, {total_pages} pages")

            return PaginatedEntityResponse(
                data=entities,
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