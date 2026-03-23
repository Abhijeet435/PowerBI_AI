import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def run_query(sql: str):
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )

        cursor = conn.cursor()
        cursor.execute(sql)

        df = cursor.fetch_pandas_all()
        rows = df.to_dict(orient='records')
        return rows

    except Exception as e:
        return {"error": str(e)}

    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    from yml_parser import load_schema
    from nl2sql_agent import generate_sql

    schema = load_schema('AdventureWorks.yml')

    question = "Top 5 products by total sales amount"
    print(f"Question: {question}")

    sql = generate_sql(question, schema)
    print(f"\nGenerated SQL:\n{sql}\n")

    rows = run_query(sql)
    print("Result rows:")
    for row in rows:
        print(row)