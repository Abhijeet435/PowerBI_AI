import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from datetime import datetime
import httpx

from yml_parser import load_schema
from nl2sql_agent import generate_sql
from snowflake_executor import run_query

load_dotenv()

app = FastAPI()

# CORS — required so browser can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request model
class QueryRequest(BaseModel):
    question: str


# ── Power BI Streaming Push ──────────────────────────────────────────────────

POWERBI_PUSH_URL = os.getenv("POWERBI_PUSH_URL")

async def push_to_powerbi(rows: list, columns: list, question: str):
    """
    Pushes query results to a Power BI Streaming Dataset.
    Expects the dataset to have fields: label, value, query_text, timestamp
    Uses the first column as label and second column as value.
    """
    if not POWERBI_PUSH_URL:
        print("⚠ POWERBI_PUSH_URL not set — skipping Power BI push")
        return

    if len(columns) < 2:
        print("⚠ Not enough columns to push to Power BI — need at least 2")
        return

    try:
        payload = [
            {
                "label": str(row[columns[0]]),
                "value": float(row[columns[1]]) if row[columns[1]] is not None else 0.0,
                "query_text": question,
                "timestamp": datetime.utcnow().isoformat()
            }
            for row in rows
        ]

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(POWERBI_PUSH_URL, json=payload)
            if response.status_code == 200:
                print(f"✅ Pushed {len(payload)} rows to Power BI")
            else:
                print(f"⚠ Power BI push returned {response.status_code}: {response.text}")

    except Exception as e:
        # Non-fatal — log and continue, don't break the API response
        print(f"⚠ Power BI push failed: {e}")


# ── Routes ───────────────────────────────────────────────────────────────────

@app.post("/query")
async def query(request: QueryRequest):
    try:
        # Step 1 — Load schema
        schema = load_schema("AdventureWorks.yml")

        # Step 2 — Generate SQL
        sql = generate_sql(request.question, schema)

        # Step 3 — Run SQL on Snowflake
        rows = run_query(sql)

        # Check if executor returned an error
        if isinstance(rows, dict) and "error" in rows:
            return {
                "success": False,
                "error": rows["error"],
                "sql": sql
            }

        # Get column names from first row
        columns = list(rows[0].keys()) if rows else []

        # Step 4 — Push results to Power BI Streaming Dataset (non-blocking)
        await push_to_powerbi(rows, columns, request.question)

        return {
            "success": True,
            "sql": sql,
            "columns": columns,
            "data": rows
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "sql": ""
        }


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "AI Q&A Tool is running!"}
