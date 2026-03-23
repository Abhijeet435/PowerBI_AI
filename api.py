import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

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


@app.get("/")
async def root():
    return {"message": "AI Q&A Tool is running!"}
