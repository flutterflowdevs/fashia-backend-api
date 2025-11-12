from typing import List, Optional
from fastapi import HTTPException
from app.db.session import get_db_connection
from app.models.facility_model import FacilityResponse, PaginatedEntityResponse
import asyncio
import json
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
)
logger = logging.getLogger(__name__)

def to_title_case(text: str) -> str:
    """Convert text to title case"""
    if not text:
        return text
    return text.title()

async def execute_subquery(conn, query: str, params: List, field_name: str):
    """Execute a subquery and return the result with field name"""
    query_start = time.time()
    try:
        cursor = await conn.execute(query, params)
        result = await cursor.fetchone()
        query_time = time.time() - query_start
        logger.info(f"✓ Subquery '{field_name}' completed in {query_time:.4f}s")
        return field_name, result[0] if result else None, query_time
    except Exception as e:
        query_time = time.time() - query_start
        logger.error(f"✗ Error in subquery '{field_name}' after {query_time:.4f}s: {e}")
        return field_name, None, query_time

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
    function_start = time.time()
    logger.info(f"🔍 Starting get_facilities_data - Page: {page}, Per-page: {per_page}, Sort: {sort_by} {sort_order}")
    logger.info(f"   Filters - Name: {name}, Cities: {cities}, States: {states}, Types: {types}, Subtypes: {subtypes}")
    logger.info(f"   Provider Filters - FirstName: {provider_first_name}, LastName: {provider_last_name}")
    logger.info(f"   Location Filters - Address: {address}, Zipcode: {zipcode}, Coords: {coords}")
    
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
        sort_by = "name"
    
    sort_order = sort_order.upper()
    if sort_order not in ["ASC", "DESC"]:
        sort_order = "ASC"
    
    try:
        async with get_db_connection() as conn:
            logger.info("📦 Database connection established")
            
            # Set query optimizations
            pragma_start = time.time()
            await conn.execute("PRAGMA temp_store = MEMORY")
            await conn.execute("PRAGMA cache_size = -64000")
            logger.info(f"   Pragmas set in {(time.time() - pragma_start):.4f}s")
            
            # Build base filters
            filter_start = time.time()
            entity_params = []
            filters = ["e.is_employer = 0"]

            # Name contains search
            if name:
                filters.append("LOWER(e.name) LIKE ?")
                entity_params.append(f"%{name.lower()}%")
                logger.debug(f"   Added name filter: {name}")
            
            if cities:
                city_conditions = []
                for city in cities:
                    city_conditions.append("LOWER(e.city) = ?")
                    entity_params.append(city.lower())
                filters.append(f"({' OR '.join(city_conditions)})")
                logger.debug(f"   Added {len(cities)} city filters")
            
            if states:
                state_conditions = []
                for state in states:
                    state_conditions.append("LOWER(s.state_name) = ?")
                    entity_params.append(state.lower())
                filters.append(f"({' OR '.join(state_conditions)})")
                logger.debug(f"   Added {len(states)} state filters")
            
            if address:
                filters.append("LOWER(e.address) LIKE ?")
                entity_params.append(f"%{address.lower()}%")
                logger.debug(f"   Added address filter: {address}")
            
            if zipcode:
                filters.append("e.zip_code = ?")
                entity_params.append(zipcode)
                logger.debug(f"   Added zipcode filter: {zipcode}")
            
            if types:
                type_conditions = []
                for facility_type in types:
                    type_conditions.append("LOWER(e.type) = ?")
                    entity_params.append(facility_type.lower())
                filters.append(f"({' OR '.join(type_conditions)})")
                logger.debug(f"   Added {len(types)} type filters")
            
            if subtypes:
                subtype_conditions = []
                for subtype in subtypes:
                    subtype_conditions.append("LOWER(e.subtype) = ?")
                    entity_params.append(subtype.lower())
                filters.append(f"({' OR '.join(subtype_conditions)})")
                logger.debug(f"   Added {len(subtypes)} subtype filters")

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
                    logger.debug(f"   Added coordinate filter: lat[{lat_min}, {lat_max}], lng[{lng_min}, {lng_max}]")

            logger.info(f"✓ Filter building completed in {(time.time() - filter_start):.4f}s - Total {len(entity_params)} params")

            # Build provider filter conditions and parameters separately
            provider_build_start = time.time()
            provider_conditions = []
            provider_params = []
            
            if provider_first_name and provider_last_name:
                provider_conditions.append("(LOWER(pe.first_name) = LOWER(?) AND LOWER(pe.last_name) = LOWER(?))")
                provider_params.extend([provider_first_name, provider_last_name])
            elif provider_first_name:
                provider_conditions.append("LOWER(pe.first_name) = LOWER(?)")
                provider_params.append(provider_first_name)
            elif provider_last_name:
                provider_conditions.append("LOWER(pe.last_name) = LOWER(?)")
                provider_params.append(provider_last_name)
            
            logger.debug(f"   Provider conditions built in {(time.time() - provider_build_start):.4f}s")
            
            # Build COMBINED filter for roles, specialties, and provider_count subqueries
            combined_build_start = time.time()
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
                logger.debug(f"   Added {len(roles)} role conditions")
            
            # Add specialty conditions
            if specialties:
                for specialty in specialties:
                    combined_conditions.append("LOWER(rsc.specialty) = LOWER(?)")
                    combined_params.append(specialty)
                logger.debug(f"   Added {len(specialties)} specialty conditions")
            
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
                logger.debug(f"   Added {len(employers)} employer conditions")
            
            employer_where_clause = " AND ".join(employer_combined_conditions) if employer_combined_conditions else "1=1"
            
            logger.info(f"✓ Combined filter building completed in {(time.time() - combined_build_start):.4f}s")

            # Combine all provider-related conditions for the main filter
            filters_build_start = time.time()
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
                logger.debug(f"   Added provider EXISTS clause")
            
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
                logger.debug(f"   Added employer EXISTS clause")

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            logger.info(f"✓ Filter composition completed in {(time.time() - filters_build_start):.4f}s - {len(filters)} filters")

            # Step 1: Get total count
            count_start = time.time()
            count_query = f"""
                SELECT COUNT(DISTINCT e.ccn_or_npi) 
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
                {where_clause}
            """
            logger.debug(f"   Count query: {count_query[:100]}...")
            count_cursor = await conn.execute(count_query, entity_params)
            total_count_row = await count_cursor.fetchone()
            total_count = total_count_row[0] if total_count_row else 0
            count_time = time.time() - count_start
            logger.info(f"✓ Count query completed in {count_time:.4f}s - Found {total_count} total facilities")

            if total_count == 0:
                logger.warning("⚠️ No facilities found matching the criteria")
                return PaginatedEntityResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=0,
                    total_pages=0,
                )

            # Step 2: Build the main query WITHOUT parameterized subqueries
            main_query_build_start = time.time()
            offset = (page - 1) * per_page
            
            # Build consistent ordering for display in JSON arrays
            if sort_by == "role":
                role_display_order = f"ORDER BY rsc.role {sort_order}"
                specialty_display_order = "ORDER BY rsc.specialty ASC"
                employer_display_order = "ORDER BY emp.name ASC"
            elif sort_by == "specialty":
                role_display_order = "ORDER BY rsc.role ASC"
                specialty_display_order = f"ORDER BY rsc.specialty {sort_order}"
                employer_display_order = "ORDER BY emp.name ASC"
            elif sort_by == "employer":
                role_display_order = "ORDER BY rsc.role ASC"
                specialty_display_order = "ORDER BY rsc.specialty ASC"
                employer_display_order = f"ORDER BY emp.name {sort_order}"
            else:
                role_display_order = "ORDER BY rsc.role ASC"
                specialty_display_order = "ORDER BY rsc.specialty ASC"
                employer_display_order = "ORDER BY emp.name ASC"

            logger.debug(f"   Sort order configured - role: {role_display_order}, specialty: {specialty_display_order}, employer: {employer_display_order}")

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
            logger.debug(f"   Actual WHERE clauses built")

            # Define sorting expressions for fields that can be null/empty
            provider_employer_base_query = f"""
                FROM provider_entities pe
                JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
                JOIN roles_specialties_classification rs ON rs.nucc_code = pt.nucc_code
                WHERE pe.npi_or_ccn = e.ccn_or_npi
                AND {actual_combined_where}
            """

            # Base query for employer joins  
            employer_base_query = f"""
                FROM provider_facility_employer_linked pfel
                JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
                JOIN roles_specialties_classification rs ON rs.nucc_code = pt.nucc_code
                WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                AND {actual_employer_where}
            """

            role_specialty_base_query = f"""
                SELECT rs.role, rs.specialty
                {provider_employer_base_query}
            """

            nulls_last_expr = {
                "role": f"""
                    (SELECT role FROM ({role_specialty_base_query} ORDER BY role {sort_order} LIMIT 1))
                """,
                "specialty": f"""
                    (SELECT specialty FROM ({role_specialty_base_query} ORDER BY specialty {sort_order} LIMIT 1))
                """,
                "employer": f"""
                    (SELECT emp.name {employer_base_query} ORDER BY emp.name {sort_order} LIMIT 1)
                """,
                "provider_count": f"""
                    (SELECT COUNT(DISTINCT pe.provider_id) {provider_employer_base_query})
                """
            }

            # Build order by clause
            if sort_by in nulls_last_expr:
                order_by_clause = f"""
                    CASE WHEN {nulls_last_expr[sort_by]} IS NULL OR {nulls_last_expr[sort_by]} = '' OR {nulls_last_expr[sort_by]} = 0 THEN 1 ELSE 0 END ASC,
                    {nulls_last_expr[sort_by]} {sort_order}
                """
            else:
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

            logger.info(f"✓ Main query build phase completed in {(time.time() - main_query_build_start):.4f}s")

            # OPTIMIZED: Main query without subqueries - we'll fetch subquery data in parallel
            main_exec_start = time.time()
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
                {where_clause}
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
            """
            
            # Only pass the main entity parameters + pagination
            all_params = entity_params + [per_page, offset]
            
            logger.debug(f"   Main query params: {len(all_params)} (entity: {len(entity_params)}, pagination: 2)")
            
            # Execute main query
            cursor = await conn.execute(main_query, all_params)
            rows = await cursor.fetchall()
            main_query_time = time.time() - main_exec_start
            
            logger.info(f"✓ Main query executed in {main_query_time:.4f}s - Found {len(rows)} rows for page {page}")

            # Convert rows to basic entities
            row_conversion_start = time.time()
            basic_entities = []
            for row in rows:
                row_dict = dict(row)
                if row_dict.get('name'):
                    row_dict['name'] = to_title_case(row_dict['name'])
                if row_dict.get('city'):
                    row_dict['city'] = to_title_case(row_dict['city'])    
                if row_dict.get('address'):
                    row_dict['address'] = to_title_case(row_dict['address'])
                basic_entities.append(row_dict)
            
            logger.info(f"✓ Row conversion completed in {(time.time() - row_conversion_start):.4f}s")

            # OPTIMIZATION: Execute subqueries in parallel for each facility
            subquery_exec_start = time.time()
            total_subquery_time = 0
            entities = []
            
            logger.info(f"🔄 Starting subquery execution for {len(basic_entities)} facilities...")
            
            for entity_idx, entity in enumerate(basic_entities):
                ccn_or_npi = entity['ccn_or_npi']
                entity_subquery_start = time.time()
                
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
                        f"""
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
                        """,
                        [ccn_or_npi],
                        "employers"
                    ),
                    # Roles
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
                    # Specialties
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
                logger.debug(f"   [Entity {entity_idx + 1}/{len(basic_entities)}] Executing 4 subqueries in parallel for {ccn_or_npi}...")
                tasks = [execute_subquery(conn, query, params, field) for query, params, field in subqueries]
                results = await asyncio.gather(*tasks)
                
                # Combine results
                entity_data = entity.copy()
                json_parse_start = time.time()
                
                for field_name, result, query_time in results:
                    try:
                        if field_name == "employers" and result:
                            employers_data = json.loads(result)
                            for employer in employers_data:
                                if employer.get('name'):
                                    employer['name'] = to_title_case(employer['name'])
                            entity_data[field_name] = employers_data
                            logger.debug(f"      {field_name}: {len(employers_data)} items parsed")
                        elif field_name in ["roles", "specialties"] and result:
                            parsed_data = json.loads(result)
                            entity_data[field_name] = parsed_data
                            logger.debug(f"      {field_name}: {len(parsed_data)} items parsed")
                        else:
                            entity_data[field_name] = result or 0 if field_name == "providers_count" else result or []
                            logger.debug(f"      {field_name}: {result}")
                    except Exception as parse_error:
                        logger.error(f"      Error parsing {field_name}: {parse_error}")
                        entity_data[field_name] = 0 if field_name == "providers_count" else []
                
                json_parse_time = time.time() - json_parse_start
                entity_total_time = time.time() - entity_subquery_start
                total_subquery_time += entity_total_time
                
                entities.append(FacilityResponse(**entity_data))
                logger.info(f"   [Entity {entity_idx + 1}/{len(basic_entities)}] ✓ Completed in {entity_total_time:.4f}s (json parsing: {json_parse_time:.4f}s)")

            logger.info(f"✓ All subqueries completed - Total time: {total_subquery_time:.4f}s, Average per entity: {total_subquery_time/len(entities):.4f}s")

            total_pages = (total_count + per_page - 1) // per_page
            total_time = time.time() - function_start

            logger.info(f"✓ Function completed successfully:")
            logger.info(f"   Total time: {total_time:.4f}s")
            logger.info(f"   Results: {len(entities)} entities, {total_pages} total pages")
            logger.info(f"   Breakdown - Count query: {count_time:.4f}s, Main query: {main_query_time:.4f}s, Subqueries: {total_subquery_time:.4f}s")

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
        total_time = time.time() - function_start
        logger.error(f"✗ Database error after {total_time:.4f}s:")
        logger.error(f"   Details: {error_details}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")