import time
import logging
from sqlalchemy import create_engine, text
import dlt

# Engine para log no Postgres local
engine_log = create_engine(
    f"postgresql+psycopg2://{dlt.secrets['local_postgres.destination.postgres.credentials']['username']}:"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['password']}@"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['host']}:5432/"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['database']}"
)

# Função de retry genérica
def retry(func, attempts=3, delay=5, step=""):
    for attempt in range(1, attempts+1):
        try:
            return func()
        except Exception as e:
            logging.warning(f"{step} - Attempt {attempt} failed: {e}")
            if attempt < attempts:
                logging.info(f"{step} - Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"{step} - All attempts failed: {e}")
                raise

# Função para gravar logs no banco
def log_execution_process(step, status, message, engine=engine_log):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.log_execution (
                id SERIAL PRIMARY KEY,
                step VARCHAR(100),
                status VARCHAR(20),
                message TEXT,
                execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.execute(
            text("""
                INSERT INTO public.log_execution (step, status, message)
                VALUES (:step, :status, :message)
            """),
            {"step": step, "status": status, "message": message}
        )
