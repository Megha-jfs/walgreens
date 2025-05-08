from datetime import datetime

def log_to_job_execution_detail(spark, batch_id, proj_id, dept_id, job_id, parent_job_id, status, error_msg="", workflow_id="", run_id="", run_url=""):
    """Update job execution status in job_execution_detail table"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Check if record exists
        job_exists_df = spark.sql(f"""
            SELECT 1 FROM data_migration_validator_dev.test_megha.job_execution_detail
            WHERE batch_id = {batch_id} AND job_id = '{job_id}' AND run_id = '{run_id}'
        """)
        
        if job_exists_df.count() > 0:
            # Update existing record
            update_sql = f"""
                UPDATE data_migration_validator_dev.test_megha.job_execution_detail
                SET status = '{status}'
            """
            
            if status in ["IN_PROGRESS"]:
                update_sql += f", start_time = '{current_time}'"
            
            if status in ["COMPLETED", "FAILED"]:
                update_sql += f", end_time = '{current_time}'"
            
            if error_msg:
                update_sql += f", error_message = '{error_msg}'"
                
            update_sql += f" WHERE batch_id = {batch_id} AND job_id = '{job_id}'" AND run_id = '{run_id}'"
            
            spark.sql(update_sql)
        else:        
            spark.sql(f"""
                INSERT INTO data_migration_validator_dev.test_megha.job_execution_detail
                (batch_id, proj_id, dept_id, job_id, parent_job_id, status, start_time, end_time, error_message, workflow_id, run_id, run_url)
                VALUES
                ({batch_id}, '{proj_id}', '{dept_id}', '{job_id}', '{parent_job_id}', '{status}', '{current_time}', NULL, '{error_msg}', '{workflow_id}', '{run_id}', '{run_url}')
            """)
        
        print(f"Job status updated successfully for batch_id: {batch_id}, job_id: {job_id}, run_id: {run_id}")
        return True
    except Exception as e:
        print(f"Error updating job status: {str(e)}")
        return False