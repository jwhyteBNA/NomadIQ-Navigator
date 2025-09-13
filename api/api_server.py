from typing import Optional
from fastapi import FastAPI
from src.logger import logger_setup
from src.utilities import duckdb_setup, ducklake_init

app = FastAPI()
logger = logger_setup("api.log")

DATA_PATH = "data/"
CATALOG_PATH = "catalog.ducklake"

conn = duckdb_setup(read_only=True)
ducklake_init(conn, DATA_PATH, CATALOG_PATH)

@app.get("/park_profile")
def get_park_profile(name: Optional[str] = None, state: Optional[str] = None, designation: Optional[str] = None):
    """
    Returns park profile information, optionally filtered by park name and/or national designation (case-insensitive, partial match).
    """
    try:
        base_query = "SELECT * FROM CURATED.NPS_PARK_PROFILE"
        params = []
        conditions = []
        if name:
            conditions.append("LOWER(name) LIKE ?")
            params.append(f"%{name.lower()}%")
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
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /park_profile endpoint: {e}")
        return {"error": str(e)}

@app.get("/park_alerts")
def get_park_alerts(park_name: Optional[str] = None, category: Optional[str] = None):
    """
    Returns park alerts, optionally filtered by park name and alert category (case-insensitive, partial match).
    """
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
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /park_alerts endpoint: {e}")
        return {"error": str(e)}

@app.get("/nps_distances")
def get_nps_distances(starting_national_park: Optional[str] = None):
    """
    Finds the distances between national parks.
    Optionally filter by starting national park (case-insensitive, partial match).
    """
    try:
        if starting_national_park:
            query = "SELECT * FROM CURATED.NPS_DISTANCES WHERE LOWER(starting_national_park) LIKE ?"
            param = f"%{starting_national_park.lower()}%"
            result = conn.execute(query, [param]).fetchdf()
        else:
            query = "SELECT * FROM CURATED.NPS_DISTANCES"
            result = conn.execute(query).fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_distances endpoint: {e}")
        return {"error": str(e)}
    
@app.get("/nps_parks_to_landmarks")
def get_nps_parks_to_landmarks(
    park_name: Optional[str] = None,
    property_name: Optional[str] = None,
    landmark_city: Optional[str] = None,
    landmark_county: Optional[str] = None,
    landmark_state: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    Finds parks and their associated landmarks, with optional filters for park name, property name, city, county, and state (all case-insensitive, partial match).
    Supports pagination with limit and offset.
    """
    try:
        base_query = "SELECT park_name, property_name, landmark_city, landmark_county, landmark_state FROM CURATED.NPS_PARKS_TO_LANDMARKS"
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
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        query += f" LIMIT {limit} OFFSET {offset}"
        result = conn.execute(query, params).fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_parks_to_landmarks endpoint: {e}")
        return {"error": str(e)}

@app.get("/nps_park_usage_annual")
def get_nps_park_usage_annual(park_name: Optional[str] = None, year: Optional[int] = None):
    """
    Returns annual usage statistics for national parks.
    Optionally filter by park name (case-insensitive, partial match) and year.
    """
    try:
        base_query = "SELECT * FROM CURATED.NPS_PARK_USAGE_ANNUAL"
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"%{park_name.lower()}%")
        if year:
            conditions.append("year = ?")
            params.append(year)
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_park_usage_annual endpoint: {e}")
        return {"error": str(e)}
    
@app.get("/park_usage_summarized")
def get_park_usage_summarized(park_name: Optional[str] = None, year: Optional[int] = None):
    """
    Returns summarized park usage statistics.
    Optionally filter by park name (case-insensitive, partial match) and year.
    """
    try:
        base_query = "SELECT * FROM CURATED.PARK_USAGE_SUMMARIZED"
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"%{park_name.lower()}%")
        if year:
            conditions.append("year = ?")
            params.append(year)
        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            query = base_query
        result = conn.execute(query, params).fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /park_usage_summarized endpoint: {e}")
        return {"error": str(e)}

@app.get("/nps_to_state_distance")
def get_nps_to_state_distance(national_park_name: Optional[str] = None, state_park_name: Optional[str] = None):
    """
    Returns distances from national parks to state parks.
    Optionally filter by national park name and state park name (case-insensitive, partial match).
    """
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
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_to_state_distance endpoint: {e}")
        return {"error": str(e)}

@app.get("/park_alert_categories")
def get_alert_categories():
    """
    Lists all distinct alert categories available for park alerts.
    """
    try:
        query = "SELECT DISTINCT alert_category FROM CURATED.PARK_ALERTS ORDER BY alert_category"
        result = conn.execute(query).fetchdf()
        return [row["alert_category"] for row in result.to_dict(orient="records")]
    except Exception as e:
        import logging
        logging.error(f"Error in /park_alert_categories endpoint: {e}")
        return {"error": str(e)}