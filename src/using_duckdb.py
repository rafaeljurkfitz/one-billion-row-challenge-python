import duckdb
import time
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

DATA_PATH = str(os.getenv("DATA_PATH"))


def create_duckdb():
    query = f"""
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3,1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("{DATA_PATH}", AUTO_DETECT=FALSE, sep=';', columns={{'station': 'VARCHAR',
        'temperature': 'DECIMAL(3,1)'}})
        GROUP BY station
        ORDER BY station
    """
    duckdb.sql(query).show()
    
    
if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")