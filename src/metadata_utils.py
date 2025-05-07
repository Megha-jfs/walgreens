import json

def get_job_details(spark, esp_job_id):
    """Get parent and child job details from data_migration_validator_dev.test_megha.job_parameter_detail table for a given esp_job_id"""
    try:
        # Get parent job details
        parent_df = spark.sql(f"""
            SELECT proj_id, dept_id, job_id, parameter_value
            FROM data_migration_validator_dev.test_megha.job_parameter_detail
            WHERE esp_job_id = '{esp_job_id}' 
            AND job_type = 'parent'
            ORDER BY invocation_id
        """)
        
        if parent_df.count() == 0:
            print(f"No parent job found for esp_job_id: {esp_job_id}")
            return None, None
        
        parent_row = parent_df.first()
        
        print(f"parent_row: {parent_row}")

        # Set job param variables
        proj_id = parent_row.proj_id
        dept_id = parent_row.dept_id
        job_id = parent_row.job_id

        print(f"proj_id: {proj_id}")
        print(f"dept_id: {dept_id}")
        print(f"job_id: {job_id}")

        print(type(parent_row.parameter_value))

        # Get job parameters from parameter_value if it exists
        job_params = {}
        if parent_row.parameter_value:
            try:
                job_params = json.loads(parent_row.parameter_value)
            except json.JSONDecodeError:
                print(f"Error parsing job parameters: {parent_row.parameter_value}")
        
        print(f"job_params: {job_params}")
        print(type(job_params))

        # Get all child jobs
        child_df = spark.sql(f"""
            SELECT job_id, invocation_id, use_runner, notebook_path, 
                  cluster_details, dependent_libraries, parameter_type, parameter_value
            FROM data_migration_validator_dev.test_megha.job_parameter_detail
            WHERE esp_job_id = '{esp_job_id}' 
            AND job_type = 'child'
            ORDER BY invocation_id
        """)
        
        child_jobs = []
        for row in child_df.collect():
            # Parse task parameters if they exist
            task_params = {}
            if row.parameter_value:
                try:
                    task_params = json.loads(row.parameter_value)
                except:
                    print(f"Error parsing task parameters: {row.parameter_value}")
            
            print(f"task_params: {task_params}")

            # Parse cluster details if they exist
            cluster_info = {}
            if row.cluster_details:
                try:
                    cluster_info = json.loads(row.cluster_details)
                except:
                    print(f"Error parsing cluster details: {row.cluster_details}")
            
            print(f"cluster_info: {cluster_info}")

            # Parse dependent libraries if they exist
            libraries = []
            if row.dependent_libraries:
                try:
                    libraries = json.loads(row.dependent_libraries)
                except:
                    print(f"Error parsing dependent libraries: {row.dependent_libraries}")
            
            print(f"libraries: {libraries}")

            child_jobs.append({
                "job_id": row.job_id,
                "invocation_id": row.invocation_id,
                "use_runner": row.use_runner,
                "notebook_path": row.notebook_path,
                "cluster_info": cluster_info,
                "libraries": libraries,
                "parameter_type": row.parameter_type,
                "task_params": task_params
            })
        
        parent_info = {
            "proj_id": parent_row.proj_id,
            "dept_id": parent_row.dept_id,
            "job_id": parent_row.job_id,
            "job_params": job_params
        }
        
        return parent_info, child_jobs
    except Exception as e:
        print(f"Error getting job details: {str(e)}")
        return None, None
