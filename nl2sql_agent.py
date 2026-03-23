import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate

load_dotenv()

def generate_sql(question: str, schema: str) -> str:
    llm = ChatGroq(
        api_key=os.getenv("GROQ_API_KEY"),
        model_name=os.getenv("GROQ_MODEL", "llama3-70b-8192"),
        temperature=0
    )

    prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template="""
You are an expert SQL developer for Snowflake databases.

Below is the database schema with all available tables, columns, and relationships:
{schema}

Rules you MUST follow:
- Use ONLY tables and columns that exist in the schema above
- Use fully qualified table names in format: DATABASE.SCHEMA.TABLE
- STRICTLY use the exact number mentioned in the question for LIMIT (e.g. "top 5" = LIMIT 5, "top 10" = LIMIT 10)
- If no number is mentioned, use LIMIT 10
- Always filter out rows where name or label columns contain '[Not Applicable]', 'N/A', 'Unknown', or NULL using WHERE clause
- Write only the SQL query — no explanations, no markdown, no code blocks
- For joins, use the relationship keys defined in the schema
- Always use ORDER BY with DESC for "top" questions
- If the question cannot be answered from the schema, return: SELECT 'Cannot answer this question from available data' AS message

Question: {question}

SQL Query:
"""
    )

    # Modern LangChain style — no LLMChain needed
    chain = prompt | llm
    result = chain.invoke({"schema": schema, "question": question})

    # Clean up the response
    sql = result.content.strip()
    sql = sql.replace('```sql', '').replace('```', '').strip()

    return sql


# Test it
if __name__ == '__main__':
    from yml_parser import load_schema

    schema = load_schema('AdventureWorks.yml')

    question = "Top 5 products by total sales amount"
    print(f"Question: {question}")
    print("Generating SQL...\n")

    sql = generate_sql(question, schema)
    print("Generated SQL:")
    print(sql)