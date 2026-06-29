import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True)

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cur = conn.cursor()

cur.execute("""
    SELECT
        CURRENT_USER(),
        CURRENT_ROLE(),
        CURRENT_WAREHOUSE(),
        CURRENT_DATABASE(),
        CURRENT_SCHEMA()
""")

print(cur.fetchone())

cur.close()
conn.close()

print("Snowflake connection successful from .env.")