from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = FastAPI(title="Maintenance Backend API", description="API for querying maintenance data from PostgreSQL")

# Database connection parameters from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def get_db_connection():
    """Create and return a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def execute_query(query, params=None):
    """Execute a query and return the results as a list of dictionaries"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or {})
            results = cursor.fetchall()
            return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution error: {str(e)}")
    finally:
        conn.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to Maintenance Backend API"}

@app.get("/dead_nodes/latest")
def get_latest_dead_node(node_id: str):
    """Get the latest dead node by node_id"""
    query = """
    SELECT * FROM public.dead_nodes
    WHERE node_id = %s
    ORDER BY id DESC LIMIT 1
    """
    result = execute_query(query, (node_id,))
    if not result:
        raise HTTPException(status_code=404, detail=f"No data found for node_id: {node_id}")
    return result[0]

@app.get("/dead_nodes/verticals")
def get_vertical_names():
    """Get all distinct vertical names from dead_nodes"""
    query = "SELECT DISTINCT vertical_name FROM public.dead_nodes"
    result = execute_query(query)
    return [row["vertical_name"] for row in result if row["vertical_name"]]

@app.get("/dead_nodes/by_vertical")
def get_dead_nodes_by_vertical(
    vertical_name: str,
    hours: int = Query(3, description="Time interval in hours")
):
    """Get recent dead nodes by vertical_name within a time interval"""
    query = """
    SELECT * FROM public.dead_nodes
    WHERE vertical_name = %s
    AND timestamp >= NOW() - INTERVAL '%s hours'
    ORDER BY id DESC
    LIMIT 100
    """
    result = execute_query(query, (vertical_name, hours))
    return result

@app.get("/outlier_data")
def get_outlier_data(
    node_id: str,
    hours: int = Query(24, description="Time interval in hours")
):
    """Get outlier data by node_id within a time interval"""
    query = """
    SELECT * FROM public.outlier_data 
    WHERE node_id = %s
    AND timestamp_column >= NOW() - INTERVAL '%s hours'
    """
    result = execute_query(query, (node_id, hours))
    return result

@app.get("/frequency_analysis")
def get_frequency_analysis(
    node: str,
    hours: int = Query(24, description="Time interval in hours")
):
    """Get frequency analysis data by node within a time interval"""
    query = """
    SELECT * FROM frequency_analysis 
    WHERE node = %s
    AND timestamp >= NOW() - INTERVAL '%s hours'
    """
    result = execute_query(query, (node, hours))
    return result

@app.get("/nan_analysis")
def get_nan_analysis(
    node: str,
    hours: int = Query(24, description="Time interval in hours"),
    limit: int = Query(1, description="Limit for the number of records")
):
    """Get NaN analysis data by node within a time interval"""
    query = """
    SELECT * FROM nan_analysis 
    WHERE node = %s
    AND timestamp_column >= NOW() - INTERVAL '%s hours'
    LIMIT %s
    """
    result = execute_query(query, (node, hours, limit))
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
