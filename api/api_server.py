from typing import Optional
from fastapi import FastAPI
from src.logger import logger_setup
from src.utilities import duckdb_setup, ducklake_init

app = FastAPI()
logger = logger_setup("api.log")

DATA_PATH = "data/"
CATALOG_PATH = "catalog.ducklake"

conn = duckdb_setup()
ducklake_init(conn, DATA_PATH, CATALOG_PATH)

@app.get("/nps_distances")
def get_nps_distances(starting_national_park: Optional[str] = None):
    try:
        if starting_national_park:
            query = "SELECT * FROM CURATED.NPS_DISTANCES WHERE starting_national_park LIKE ?"
            param = f"%{starting_national_park}%"
            result = conn.execute(query, [param]).fetchdf()
        else:
            query = "SELECT * FROM CURATED.NPS_DISTANCES"
            result = conn.execute(query).fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_distances endpoint: {e}")
        return {"error": str(e)}

@app.get("/nps_park_usage_annual")
def get_nps_park_usage_annual(park_name: Optional[str] = None, year: Optional[int] = None):
    try:
        base_query = "SELECT * FROM CURATED.NPS_PARK_USAGE_ANNUAL"
        params = []
        conditions = []
        if park_name:
            conditions.append("park_name LIKE ?")
            params.append(f"%{park_name}%")
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

@app.get("/nps_parks_to_landmarks")
def get_nps_parks_to_landmarks(
    park_name: Optional[str] = None,
    park_state: Optional[str] = None,
    property_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    try:
        base_query = "SELECT park_name, property_name, park_state FROM CURATED.NPS_PARKS_TO_LANDMARKS"
        params = []
        conditions = []
        if park_name:
            conditions.append("LOWER(park_name) LIKE ?")
            params.append(f"{park_name.lower()}%")
        if park_state:
            conditions.append("LOWER(park_state) LIKE ?")
            params.append(f"{park_state.lower()}%")
        if property_name:
            conditions.append("LOWER(property_name) LIKE ?")
            params.append(f"{property_name.lower()}%")
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

@app.get("/nps_landmarks")
def get_nps_landmarks():
    try:
        query = "SELECT DISTINCT landmark_name FROM CURATED.NPS_PARKS_TO_LANDMARKS ORDER BY landmark_name"
        result = conn.execute(query).fetchdf()
        return [row["landmark_name"] for row in result.to_dict(orient="records")]
    except Exception as e:
        import logging
        logging.error(f"Error in /nps_landmarks endpoint: {e}")
        return {"error": str(e)}

@app.get("/nps_to_state_distance")
def get_nps_to_state_distance(park_name: Optional[str] = None, state_name: Optional[str] = None):
    try:
        base_query = "SELECT * FROM CURATED.NPS_TO_STATE_DISTANCE"
        params = []
        conditions = []
        if park_name:
            conditions.append("park_name LIKE ?")
            params.append(f"%{park_name}%")
        if state_name:
            conditions.append("state_name LIKE ?")
            params.append(f"%{state_name}%")
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
    try:
        query = "SELECT DISTINCT alert_category FROM CURATED.PARK_ALERTS ORDER BY alert_category"
        result = conn.execute(query).fetchdf()
        return [row["alert_category"] for row in result.to_dict(orient="records")]
    except Exception as e:
        import logging
        logging.error(f"Error in /park_alert_categories endpoint: {e}")
        return {"error": str(e)}

@app.get("/park_alerts")
def get_park_alerts(park_name: Optional[str] = None, category: Optional[str] = None):
    try:
        base_query = "SELECT * FROM CURATED.PARK_ALERTS"
        params = []
        conditions = []
        if park_name:
            conditions.append("park_name LIKE ?")
            params.append(f"%{park_name}%")
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

@app.get("/park_usage_annual")
def get_park_usage_annual(park_name: Optional[str] = None, year: Optional[int] = None):
    try:
        base_query = "SELECT * FROM CURATED.PARK_USAGE_ANNUAL"
        params = []
        conditions = []
        if park_name:
            conditions.append("park_name LIKE ?")
            params.append(f"%{park_name}%")
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
        logging.error(f"Error in /park_usage_annual endpoint: {e}")
        return {"error": str(e)}