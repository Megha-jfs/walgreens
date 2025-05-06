def get_child_jobs(spark, proj_id, dept_id, job_id):
    """Get child jobs and their sequence from job_parameter_detail"""
    try:
        
        result = spark.sql(f"""
            SELECT invocation_id, param_val
            FROM data_migration_validator_dev.test_megha.job_parameter_detail
            WHERE proj_id = '{proj_id}'
            AND dept_id = '{dept_id}'
            AND job_id = '{job_id}'
            AND lower(param_name) = 'childname'
            ORDER BY invocation_id
        """)

        child_jobs = []
        for row in result.collect():
            child_jobs.append({"name": row["param_val"], "sequence": int(row["invocation_id"])})

        return child_jobs
    except Exception as e:
        print(f"Error getting child jobs: {str(e)}")
        return []


def get_notebook_path(spark, proj_id, dept_id, job_id):
    """Get notebook path for a job"""
    try:
        notebook_path_df = spark.sql(f"""
            SELECT param_val
            FROM data_migration_validator_dev.test_megha.job_parameter_detail
            WHERE proj_id = '{proj_id}'
            AND dept_id = '{dept_id}'
            AND job_id = '{job_id}'
            AND lower(param_type) = 'notebook'
            AND lower(param_name) = 'path'
            ORDER BY invocation_id
        """)
        
        if notebook_path_df.count() == 0:
            print(f"No notebook path found for job {job_id}")
            return None
            
        return notebook_path_df.first()["param_val"]
    except Exception as e:
        print(f"Error getting notebook path: {str(e)}")
        return None