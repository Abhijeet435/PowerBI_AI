import os
import logging
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from yml_parser import load_schema
from nl2sql_agent import generate_sql
from snowflake_executor import run_query

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Q&A Snowflake API")

# -----------------------------
# ✅ CORS (Allow frontend + Power BI)
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production if needed
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# ✅ Allow iframe embedding (Power BI fix)
# -----------------------------
class AllowIframeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "ALLOWALL"
        response.headers["Content-Security-Policy"] = "frame-ancestors *"
        return response

app.add_middleware(AllowIframeMiddleware)

# -----------------------------
# ✅ Request Model
# -----------------------------
class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3)
    filters: Dict[str, Any] = {}

# -----------------------------
# ✅ Helper: Validate Filters
# -----------------------------
def validate_filters(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Basic whitelist validation to prevent SQL injection
    Modify allowed_fields based on your schema
    """
    allowed_fields = {"symbol", "date", "region", "product"}

    safe_filters = {}
    for key, value in filters.items():
        if key in allowed_fields:
            safe_filters[key] = value
        else:
            logger.warning(f"Ignored unsafe filter: {key}")

    return safe_filters

# -----------------------------
# ✅ Core Query Logic
# -----------------------------
def process_query(question: str, filters: Dict[str, Any]):
    try:
        # Step 1: Load schema
        schema = load_schema("AdventureWorks.yml")

        # Step 2: Validate filters
        safe_filters = validate_filters(filters)

        # Step 3: Generate SQL
        sql = generate_sql(question, schema, filters=safe_filters)
        logger.info(f"Generated SQL: {sql}")

        # Step 4: Execute SQL
        rows = run_query(sql)

        # Handle Snowflake errors
        if isinstance(rows, dict) and "error" in rows:
            logger.error(f"Snowflake Error: {rows['error']}")
            return {
                "success": False,
                "error": rows["error"],
                "sql": sql
            }

        # Extract columns
        columns = list(rows[0].keys()) if rows else []

        return {
            "success": True,
            "sql": sql,
            "columns": columns,
            "data": rows,
            "row_count": len(rows)
        }

    except Exception as e:
        logger.exception("Unhandled error in process_query")
        return {
            "success": False,
            "error": str(e),
            "sql": ""
        }

# -----------------------------
# ✅ POST Endpoint (Main API)
# -----------------------------
@app.post("/query")
async def query_post(request: QueryRequest):
    return process_query(request.question, request.filters)

# -----------------------------
# ✅ GET Endpoint (Power BI friendly)
# -----------------------------
@app.get("/query")
async def query_get(
    question: str = Query(..., min_length=3),
    symbol: Optional[str] = None,
    date: Optional[str] = None,
    region: Optional[str] = None,
    product: Optional[str] = None
):
    filters = {}

    if symbol:
        filters["symbol"] = symbol
    if date:
        filters["date"] = date
    if region:
        filters["region"] = region
    if product:
        filters["product"] = product

    return process_query(question, filters)

# -----------------------------
# ✅ Health Check
# -----------------------------
@app.get("/")
async def root():
    return {
        "status": "running",
        "service": "AI Q&A Tool",
        "version": "1.1"
    }

# -----------------------------
# ✅ Debug Endpoint (Optional)
# -----------------------------
@app.get("/debug")
async def debug():
    return {
        "env_loaded": True,
        "message": "Debug endpoint working"
    }
