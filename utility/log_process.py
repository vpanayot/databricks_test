from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, date
import uuid


def init_run(spark, process_name):

    v_batch_run_row = spark.sql("SELECT BatchRunID, ProcessingDate FROM meta.etl_batch_run WHERE ActiveInd = 'Y'").first()
    v_batch_run_id = v_batch_run_row["BatchRunID"]
    v_process_start_time = datetime.now()
    v_process_name = process_name
    v_valid_from_dttm = v_batch_run_row["ProcessingDate"]
    v_max_valid_to_dttm = date(9999, 12, 31)
    v_run_id = str(uuid.uuid4())

    # Log process start
    df_run_log = spark.sql("""
        INSERT INTO meta.etl_run_log (
            RunID,
            BatchRunID,
            ProcessName,
            RunStartTime,
            RunEndTime,
            RunStatus
        )
        VALUES (
            :run_id,
            :batch_run_id,
            :process_name,
            :start_time,
            NULL,
            'Running'
        )
        """
        , args = { "run_id": v_run_id, "batch_run_id": v_batch_run_id, "process_name": v_process_name, "start_time": v_process_start_time }
    )

    return {
        "run_id": v_run_id,
        "process_start_time": v_process_start_time,
        "valid_from_dttm": v_valid_from_dttm,
        "max_valid_to_dttm": v_max_valid_to_dttm
    }

def finalize_run(spark, run_id, status):

    if run_id is not None:

        v_process_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if status == "success":
            
            # Log process as completed with success
            spark.sql("""
                UPDATE meta.etl_run_log 
                SET
                    RunEndTime = :process_end_time,
                    RunStatus = 'Succeeded'
                WHERE RunID = :run_id
                """
                , args = { "process_end_time": v_process_end_time, "run_id": run_id }
            )
        else:

            # Log process as completed with failure
            spark.sql("""
                UPDATE meta.etl_run_log 
                SET
                    RunEndTime = :process_end_time,
                    RunStatus = 'Failed'
                WHERE RunID = :run_id
                """
                , args = { "process_end_time": v_process_end_time, "run_id": run_id }
            )

    else:
        raise ValueError("Finalize process failed. 'run_id' is empty.")