from typing import Optional
import pandas as pd
from fastapi import FastAPI
from src.logger import logger_setup
from src.utilities import duckdb_setup, ducklake_init

app = FastAPI()
logger = logger_setup("api.log")

DATA_PATH = "data/"
CATALOG_PATH = "catalog.ducklake"

conn = duckdb_setup(read_only=True)
ducklake_init(conn, DATA_PATH, CATALOG_PATH)

@app.get("/landmarks", tags=["Landmarks"])
def get_all_landmarks(state: Optional[str] = None, city: Optional[str] = None):
    """
    Returns all landmarks with full details. Optionally filter by state and city (case-insensitive, partial match).
    """
    logger.info(f"/landmarks called with state={state}, city={city}")
    try:
        base_query = "SELECT * FROM CURATED.NATL_LANDMARKS"
        params = []
        conditions = []
        if state:
            conditions.append("(LOWER(state) LIKE ? OR LOWER(state_abbr) LIKE ?)")
            params.extend([f"%{state.lower()}%", f"%{state.lower()}%"])
        if city:
            conditions.append("LOWER(city) LIKE ?")
            params.append(f"%{city.lower()}%")
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        result = result.replace([pd.NA, pd.NaT, float('nan'), float('inf'), -float('inf')], None)
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /landmarks endpoint: {e}")
        return {"error": str(e)}

@app.get("/landmarks/summary", tags=["Landmarks"])
def get_landmarks_summary(state: Optional[str] = None, state_abbr: Optional[str] = None):
    """
    Returns summary statistics for landmarks: counts by state, state_abbr, category_of_property, and level_of_significance.
    Optionally filter by state or state_abbr (case-insensitive, partial match).
    """
    try:
        params = []
        conditions = []
        if state:
            conditions.append("LOWER(state) LIKE ?")
            params.append(f"%{state.lower()}%")
        if state_abbr:
            conditions.append("LOWER(state_abbr) LIKE ?")
            params.append(f"%{state_abbr.lower()}%")
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        state_query = f"""
            SELECT state, state_abbr, COUNT(*) AS count
            FROM CURATED.NATL_LANDMARKS
            {where_clause}
            GROUP BY state, state_abbr
            ORDER BY count DESC
        """
        category_query = f"""
            SELECT state, state_abbr, category_of_property, COUNT(*) AS count
            FROM CURATED.NATL_LANDMARKS
            {where_clause}
            GROUP BY state, state_abbr, category_of_property
            ORDER BY state, count DESC
        """
        level_query = f"""
            SELECT state, state_abbr, level_of_significance, COUNT(*) AS count
            FROM CURATED.NATL_LANDMARKS
            {where_clause}
            GROUP BY state, state_abbr, level_of_significance
            ORDER BY state, count DESC
        """
        state_stats = conn.execute(state_query, params).fetchdf().to_dict(orient="records")
        category_stats = conn.execute(category_query, params).fetchdf().to_dict(orient="records")
        level_stats = conn.execute(level_query, params).fetchdf().to_dict(orient="records")
        return {
            "by_state": state_stats,
            "by_category": category_stats,
            "by_level": level_stats
        }
    except Exception as e:
        logger.error(f"Error in /landmarks/summary endpoint: {e}")
        return {"error": str(e)}


@app.get("/parks", tags=["National Parks"])
def get_park_profile(name: Optional[str] = None, park_code: Optional[str] = None, state: Optional[str] = None,  designation: Optional[str] = None):
    """
    Returns park profile information, optionally filtered by park name and/or national designation (case-insensitive, partial match).
    """
    logger.info(f"/parks called with name={name}, park_code={park_code}, state={state}, designation={designation}")
    try:
        base_query = "SELECT * FROM CURATED.NPS_PARK_PROFILE"
        params = []
        conditions = []
        if name:
            conditions.append("LOWER(name) LIKE ?")
            params.append(f"%{name.lower()}%")
        if park_code:
            conditions.append("LOWER(park_code) LIKE ?")
            params.append(f"%{park_code.lower()}%")
        if state:
            conditions.append("LOWER(states) LIKE ?")
            params.append(f"%{state.lower()}%")
        if designation:
            conditions.append("LOWER(designation) LIKE ?")
            params.append(f"%{designation.lower()}%")
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        logger.info(f"/park_profile query: {query} params: {params}")
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /park_profile endpoint: {e}")
        return {"error": str(e)}
    

@app.get("/parks/alerts", tags=["National Parks"])
def get_park_alerts(park_name: Optional[str] = None, category: Optional[str] = None):
    """
    Returns park alerts, optionally filtered by park name and alert category (case-insensitive, partial match).
    """
    logger.info(f"/parks/alerts called with park_name={park_name}, category={category}")
    try:
        base_query = "SELECT * FROM CURATED.PARK_ALERTS"
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"%{park_name.lower()}%")
        if category:
            conditions.append("LOWER(alert_category) LIKE ?")
            params.append(f"%{category.lower()}%")
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        # Filter out alerts where alert_title is null
        if "alert_title" in result.columns:
            result = result[result["alert_title"].notnull()]
        logger.info(f"/parks/alerts query: {query} params: {params}")
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /parks/alerts endpoint: {e}")
        return {"error": str(e)}

@app.get("/parks/distances", tags=["National Parks"])
def get_nps_distances(starting_national_park: Optional[str] = None):
    """
    Finds the distances between national parks.
    Optionally filter by starting national park (case-insensitive, partial match).
    """
    logger.info(f"/parks/distances called with starting_national_park={starting_national_park}")
    try:
        if starting_national_park:
            query = "SELECT * FROM CURATED.NPS_DISTANCES WHERE LOWER(starting_national_park) LIKE ?"
            param = f"%{starting_national_park.lower()}%"
            result = conn.execute(query, [param]).fetchdf()
        else:
            query = "SELECT * FROM CURATED.NPS_DISTANCES"
            result = conn.execute(query).fetchdf()
        logger.info(f"/parks/distances query: {query}")
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /parks/distances endpoint: {e}")
        return {"error": str(e)}
    
@app.get("/parks/landmarks", tags=["National Parks"])
def get_nps_parks_to_landmarks(
    park_name: Optional[str] = None,
    property_name: Optional[str] = None,
    landmark_city: Optional[str] = None,
    landmark_county: Optional[str] = None,
    landmark_state: Optional[str] = None,
    level_of_significance: Optional[str] = None,        
    area_of_significance: Optional[str] = None,          
    category_of_property: Optional[str] = None,     
    limit: int = 5000,
    offset: int = 0
):
    """
    Finds parks and their associated landmarks, with optional filters for park name, property name, city, county, and state, as well as area of significance, level of significance, and category of property (all case-insensitive, partial match).
    Supports pagination with limit and offset.
    """
    logger.info(f"/parks/landmarks called with park_name={park_name}, property_name={property_name}, landmark_city={landmark_city}, landmark_county={landmark_county}, landmark_state={landmark_state}, level_of_significance={level_of_significance}, area_of_significance={area_of_significance}, category_of_property={category_of_property}, limit={limit}, offset={offset}")
    try:
        base_query = "SELECT park_name, property_name AS nearby_landmark, landmark_address AS address, landmark_city AS city, landmark_county AS county, landmark_state AS state, listed_date, level_of_significance, area_of_significance, category_of_property FROM CURATED.NPS_PARKS_TO_LANDMARKS"
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"%{park_name.lower()}%")
        if property_name:
            conditions.append("LOWER(property_name) LIKE ?")
            params.append(f"%{property_name.lower()}%")
        if landmark_city:
            conditions.append("LOWER(landmark_city) LIKE ?")
            params.append(f"%{landmark_city.lower()}%")
        if landmark_county:
            conditions.append("LOWER(landmark_county) LIKE ?")
            params.append(f"%{landmark_county.lower()}%")
        if landmark_state:
            conditions.append("LOWER(landmark_state) LIKE ?")
            params.append(f"%{landmark_state.lower()}%")
        if level_of_significance:
            conditions.append("LOWER(level_of_significance) LIKE ?")
            params.append(f"%{level_of_significance.lower()}%")
        if area_of_significance:
            conditions.append("LOWER(area_of_significance) LIKE ?")
            params.append(f"%{area_of_significance.lower()}%")
        if category_of_property:
            conditions.append("LOWER(category_of_property) LIKE ?")
            params.append(f"%{category_of_property.lower()}%")
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        query += f" LIMIT {limit} OFFSET {offset}"
        result = conn.execute(query, params).fetchdf()
        logger.info(f"/parks/landmarks query: {query} params: {params}")
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /parks/landmarks endpoint: {e}")
        return {"error": str(e)}


@app.get("/parks/usage", tags=["National Parks"])
def get_park_usage(
    park_name: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    granularity: str = "annual",
    aggregate: Optional[bool] = False
):
    """
    Returns park usage statistics with flexible granularity (annual or monthly).
    Optionally filter by park name, year, month, and aggregate totals for all parks.
    """
    logger.info(f"/parks/usage called with park_name={park_name}, year={year}, month={month}, granularity={granularity}, aggregate={aggregate}")
    try:
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"%{park_name.lower()}%")
        if year:
            conditions.append("year = ?")
            params.append(year)
        if granularity == "monthly" and month:
            conditions.append("month = ?")
            params.append(month)
        if granularity == "monthly":
            base_query = "SELECT * FROM CURATED.NPS_PARK_USAGE_ANNUAL"
            if conditions:
                query = base_query + " WHERE " + " AND ".join(conditions)
            else:
                query = base_query
            result = conn.execute(query, params).fetchdf()
            logger.info(f"/parks/usage monthly query: {query} params: {params}")
            return result.to_dict(orient="records")
        elif granularity == "annual":
            if aggregate and not park_name:
                where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
                query = f"""
                    SELECT year,
                           SUM(total_recreation_visits) AS total_recreation_visits,
                           SUM(total_non_recreation_visits) AS total_non_recreation_visits,
                           SUM(total_concessioner_camping) AS total_concessioner_camping,
                           SUM(total_tent_campers) AS total_tent_campers,
                           SUM(total_rv_campers) AS total_rv_campers
                    FROM CURATED.PARK_USAGE_SUMMARIZED
                    {where_clause}
                    GROUP BY year
                    ORDER BY year
                """
                result = conn.execute(query, params).fetchdf()
                logger.info(f"/parks/usage annual aggregate query: {query} params: {params}")
                result = result.fillna(0)
                return result.to_dict(orient="records")
            else:
                base_query = "SELECT * FROM CURATED.PARK_USAGE_SUMMARIZED"
                if conditions:
                    query = base_query + " WHERE " + " AND ".join(conditions)
                else:
                    query = base_query
                query += " ORDER BY total_recreation_visits DESC"
                result = conn.execute(query, params).fetchdf()
                logger.info(f"/parks/usage annual query: {query} params: {params}")
                result = result.replace([pd.NA, pd.NaT, float('nan'), float('inf'), -float('inf')], None)
                return result.to_dict(orient="records")
        else:
            return {"error": "Invalid granularity. Use 'annual' or 'monthly'."}
    except Exception as e:
        logger.error(f"Error in /parks/usage endpoint: {e}")
        return {"error": str(e)}

@app.get("/parks/state-distances", tags=["National Parks"])
def get_nps_to_state_distance(national_park_name: Optional[str] = None, state_park_name: Optional[str] = None):
    """
    Returns distances from national parks to state parks.
    Optionally filter by national park name and state park name (case-insensitive, partial match).
    """
    logger.info(f"/parks/state-distances called with national_park_name={national_park_name}, state_park_name={state_park_name}")
    try:
        base_query = "SELECT * FROM CURATED.NPS_TO_STATE_DISTANCE"
        params = []
        conditions = []
        if national_park_name:
            conditions.append("LOWER(national_park_name) LIKE ?")
            params.append(f"%{national_park_name.lower()}%")
        if state_park_name:
            conditions.append("LOWER(state_park_name) LIKE ?")
            params.append(f"%{state_park_name.lower()}%")
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        logger.info(f"/parks/state-distances query: {query} params: {params}")
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error in /parks/state-distances endpoint: {e}")
        return {"error": str(e)}