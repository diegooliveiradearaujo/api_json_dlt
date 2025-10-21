import time
import logging
import datetime
from bronze_pipeline import run_bronze_pipeline
from silver_pipeline import run_silver_pipeline
from gold_pipeline import run_gold_pipeline
from utils import retry, log_execution_process, engine_log


logging.basicConfig(
    level=logging.INFO,  #define the log to be showed
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():  
    #full pipeline
    pipelines = [
        ("Bronze", run_bronze_pipeline),
        ("Silver", run_silver_pipeline),
        ("Gold", run_gold_pipeline)
    ]

    #iteration of logs and retries for each pipeline 
    for name, func in pipelines:
        try:
            #execute the pipeline with retry
            retry(func, attempts=3, delay=10, step=f"Pipeline {name}")
            
            #log success message locally and in the database
            msg = f"Pipeline {name} executed successfully at {datetime.datetime.now()}"
            logging.info(msg)
            log_execution_process(f"pipeline_{name.lower()}", "SUCCESS", msg, engine_log)
        except Exception as e:
            #log error message locally and in the database
            error_msg = f"Pipeline {name} failed: {e}"
            logging.error(error_msg)
            log_execution_process(f"pipeline_{name.lower()}", "ERROR", error_msg, engine_log)
            #stop execution if a pipeline fails
            break

if __name__ == "__main__":
    interval_minutes = 3  #interval between pipeline runs in minutes

    #infinite loop to run pipelines periodically (orchestration)
    while True:
        logging.info(f"Starting pipelines run at {datetime.datetime.now()}")
        main()  #run all pipelines
        logging.info(f"Sleeping for {interval_minutes} minutes...")
        time.sleep(interval_minutes * 60)  #wait before next run
