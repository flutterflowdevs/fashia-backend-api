from typing import List, Optional
from fastapi import HTTPException
from app.db.session import get_db_connection
from app.models.facility_model import FacilityResponse, PaginatedEntityResponse

import json

def to_title_case(text: str) -> str:
    """Convert text to title case"""
    if not text:
        return text
    return text.title()

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
            # Build base filters
            entity_params = []
            filters = ["e.is_employer = 0"]

            # Name contains search
            if name:
                filters.append("LOWER(e.name) LIKE ?")
                entity_params.append(f"%{name.lower()}%")
            
            if cities:
                city_conditions = []
                for city in cities:
                    city_conditions.append("LOWER(e.city) = ?")
                    entity_params.append(city.lower())
                filters.append(f"({' OR '.join(city_conditions)})")
            
            if states:
                state_conditions = []
                for state in states:
                    state_conditions.append("LOWER(s.state_name) = ?")
                    entity_params.append(state.lower())
                filters.append(f"({' OR '.join(state_conditions)})")
            
            if address:
                filters.append("LOWER(e.address) LIKE ?")
                entity_params.append(f"%{address.lower()}%")
            
            if zipcode:
                filters.append("e.zip_code = ?")
                entity_params.append(zipcode)
            
            if types:
                type_conditions = []
                for facility_type in types:
                    type_conditions.append("LOWER(e.type) = ?")
                    entity_params.append(facility_type.lower())
                filters.append(f"({' OR '.join(type_conditions)})")
            
            if subtypes:
                subtype_conditions = []
                for subtype in subtypes:
                    subtype_conditions.append("LOWER(e.subtype) = ?")
                    entity_params.append(subtype.lower())
                filters.append(f"({' OR '.join(subtype_conditions)})")

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

            # Build provider filter conditions and parameters separately
            provider_conditions = []
            provider_params = []
            
            if provider_first_name and provider_last_name:
                provider_conditions.append("(LOWER(p.first_name) = LOWER(?) AND LOWER(p.last_name) = LOWER(?))")
                provider_params.extend([provider_first_name, provider_last_name])
            elif provider_first_name:
                provider_conditions.append("LOWER(p.first_name) = LOWER(?)")
                provider_params.append(provider_first_name)
            elif provider_last_name:
                provider_conditions.append("LOWER(p.last_name) = LOWER(?)")
                provider_params.append(provider_last_name)
            
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
            
            # Add specialty conditions
            if specialties:
                for specialty in specialties:
                    combined_conditions.append("LOWER(rsc.specialty) = LOWER(?)")
                    combined_params.append(specialty)
            
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
            
            employer_where_clause = " AND ".join(employer_combined_conditions) if employer_combined_conditions else "1=1"
            
            # Combine all provider-related conditions for the main filter
            if combined_conditions:
                combined_where = " AND ".join(combined_conditions)
                filters.append(f"""EXISTS (
                    SELECT 1 FROM provider_entities pe
                    INNER JOIN providers p ON p.npi = pe.provider_id
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
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
                    INNER JOIN providers p ON p.npi = pfel.provider_id
                    INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                    INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                    WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                    AND ({employer_where_clause})
                )""")
                entity_params.extend(employer_combined_params)

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""

            # Step 1: Get total count
            count_query = f"""
                SELECT COUNT(DISTINCT e.ccn_or_npi) 
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
                {where_clause}
            """
            count_cursor = await conn.execute(count_query, entity_params)
            total_count_row = await count_cursor.fetchone()
            total_count = total_count_row[0] if total_count_row else 0

            if total_count == 0:
                return PaginatedEntityResponse(
                    data=[],
                    page=page,
                    per_page=per_page,
                    total=0,
                    total_pages=0,
                )

            # Step 2: Build the main query WITHOUT parameterized subqueries
            # Instead, we'll use the actual values in the query to avoid parameter conflicts
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

            # Define sorting expressions for fields that can be null/empty
            # Create a single CTE-style approach for the sorting expressions
            # Base query for provider-employer joins
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

            # Main query with actual values instead of parameter placeholders in subqueries
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
                    e.longitude,
                    (
                        SELECT COUNT(DISTINCT pe.provider_id)
                        FROM provider_entities pe
                        INNER JOIN providers p ON p.npi = pe.provider_id
                        INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                        INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                        WHERE pe.npi_or_ccn = e.ccn_or_npi
                        AND {actual_combined_where}
                    ) AS providers_count,
                    (
                        SELECT json_group_array(json_object('name', emp.name, 'ccn_or_npi', emp.ccn_or_npi))
                        FROM (
                            SELECT DISTINCT emp.name, emp.ccn_or_npi
                            FROM provider_facility_employer_linked pfel
                            INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
                            INNER JOIN providers p ON p.npi = pfel.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi
                            AND {actual_employer_where}
                            {employer_display_order}
                        ) emp
                    ) AS employers,
                    (
                        SELECT json_group_array(role)
                        FROM (
                            SELECT DISTINCT rsc.role
                            FROM provider_entities pe
                            INNER JOIN providers p ON p.npi = pe.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pe.npi_or_ccn = e.ccn_or_npi
                            AND {actual_combined_where}
                            {role_display_order}
                        )
                    ) AS roles,
                    (
                        SELECT json_group_array(specialty)
                        FROM (
                            SELECT DISTINCT rsc.specialty
                            FROM provider_entities pe
                            INNER JOIN providers p ON p.npi = pe.provider_id
                            INNER JOIN provider_taxonomies pt ON pt.npi = p.npi
                            INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
                            WHERE pe.npi_or_ccn = e.ccn_or_npi
                            AND {actual_combined_where}
                            {specialty_display_order}
                        )
                    ) AS specialties
                FROM entities_enriched e
                LEFT JOIN states s ON s.state_id = e.state_id
                {where_clause}
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
            """
            
            # Only pass the main entity parameters + pagination
            all_params = entity_params + [per_page, offset]
            
            # Debug info
            print(f"Main query parameters: {len(all_params)}")
            print(f"Entity params: {entity_params}")
            print(f"Address search: '{address}'")
            
            cursor = await conn.execute(main_query, all_params)
            rows = await cursor.fetchall()

            # Convert rows to EntityResponse objects
            entities = []
            for row in rows:
                row_dict = dict(row)
                if row_dict.get('name'):
                    row_dict['name'] = to_title_case(row_dict['name'])
                if row_dict.get('city'):
                    row_dict['city'] = to_title_case(row_dict['city'])    
                if row_dict.get('address'):
                    row_dict['address'] = to_title_case(row_dict['address'])
                employers_data = json.loads(row_dict['employers'] if row_dict['employers'] else '[]')
                for employer in employers_data:
                    if employer.get('name'):
                        employer['name'] = to_title_case(employer['name'])
                row_dict["employers"] = employers_data
                row_dict['roles'] = json.loads(row_dict['roles'] if row_dict['roles'] else '[]')
                row_dict['specialties'] = json.loads(row_dict['specialties'] if row_dict['specialties'] else '[]')
                
                entities.append(FacilityResponse(**row_dict))

            total_pages = (total_count + per_page - 1) // per_page

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
        print(f"Database error details: {error_details}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")