# Utility functions for workflow operations using Databricks Asset Bundles
import os
import yaml
from datetime import datetime

def create_workflow_bundle(workflow_name, tasks, job_parameters=None, bundle_root="./dabs"):
    """Create a Databricks Asset Bundle with the given tasks and job-level parameters
    
    Args:
        workflow_name (str): Name for the workflow
        tasks (list): List of task configurations
        job_parameters (dict, optional): Job-level parameters to be passed to all tasks
        bundle_root (str): Root directory for the DAB files
    
    Returns:
        str: Path to the created bundle
    """
    try:
        # Create directory structure
        bundle_dir = os.path.join(bundle_root, workflow_name)
        resources_dir = os.path.join(bundle_dir, "resources")
        
        os.makedirs(resources_dir, exist_ok=True)
        
        # Create job definition file with cluster configuration
        job_config = {
            "resources": {
                "jobs": {
                    f"{workflow_name}": {
                        "name": workflow_name,
                        "job_clusters": [
                            {
                                "job_cluster_key": f"{workflow_name}_job_cluster",
                                "new_cluster": {
                                    "spark_version": "13.3.x-scala2.12",
                                    "node_type_id": "Standard_DS3_v2",
                                    "num_workers": 2,
                                    "spark_conf": {
                                        "spark.databricks.delta.preview.enabled": "true",
                                        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true"
                                    },
                                    "spark_env_vars": {
                                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                                    },
                                    "autoscale": {
                                        "min_workers": 1,
                                        "max_workers": 4
                                    }
                                }
                            }
                        ],
                        "tasks": []
                    }
                }
            }
        }
        
        # Update all tasks to use the job cluster
        for task in tasks:
            # Add job cluster reference to each task
            task["job_cluster_key"] = f"{workflow_name}_job_cluster"
            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task)
        
        # Add job parameters if provided
        if job_parameters:
            # Convert parameters to the format expected by DABs
            job_params_formatted = []
            for key, value in job_parameters.items():
                job_params_formatted.append({
                    "name": key,
                    "default": value
                })
            
            job_config["resources"]["jobs"][workflow_name]["parameters"] = job_params_formatted
        
        # Write the job configuration file
        with open(os.path.join(resources_dir, f"{workflow_name}_job.yml"), "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Successfully created workflow job config in {resources_dir}/{workflow_name}_job.yml")
        print(f"Note: Use this with your manually created databricks.yml file")
        
        return bundle_dir
    except Exception as e:
        print(f"Error creating workflow bundle: {str(e)}")
        return None

def build_workflow_tasks(proj_id, dept_id, job_id, batch_id, child_jobs, notebook_paths):
    """Build tasks for the workflow including batch check, status update, and SQL tasks"""
    tasks = []
    
    # Task 1: Batch check
    batch_check_task = {
        "task_key": "batch_check",
        "description": "Check for open batch",
        "notebook_task": {
            "notebook_path": "/Shared/ProcessControl/notebooks/batch_check",
            "base_parameters": {
                "batch_id": str(batch_id)
            }
        },
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(batch_check_task)
    
    # Task 2: Status update (mark as running)
    status_update_task = {
        "task_key": "status_update_start",
        "description": "Mark job as running",
        "notebook_task": {
            "notebook_path": "/Shared/ProcessControl/notebooks/status_update",
            "base_parameters": {
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
                "batch_id": str(batch_id),
                "status": "RUNNING"
            }
        },
        "depends_on": [
            {"task_key": "batch_check"}
        ],
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(status_update_task)
    
    # Tasks 3+: SQL execution tasks
    previous_task = "status_update_start"
    for i, child in enumerate(child_jobs):
        child_name = child["name"]
        notebook_path = notebook_paths.get(child_name)
        
        if not notebook_path:
            print(f"No notebook path found for child job {child_name} - skipping")
            continue
        
        child_task = {
            "task_key": f"sql_execution_{child_name}",
            "description": f"Execute SQL for {child_name}",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "proj_id": str(proj_id),
                    "dept_id": str(dept_id),
                    "job_id": child_name,
                    "batch_id": str(batch_id)
                }
            },
            "depends_on": [
                {"task_key": previous_task}
            ],
            "timeout_seconds": 7200,
            "retry_on_timeout": False
        }
        tasks.append(child_task)
        previous_task = f"sql_execution_{child_name}"
    
    # Final task: Status update (mark as completed)
    status_update_final_task = {
        "task_key": "status_update_complete",
        "description": "Mark job as completed",
        "notebook_task": {
            "notebook_path": "/Shared/ProcessControl/notebooks/status_update",
            "base_parameters": {
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
                "batch_id": str(batch_id),
                "status": "COMPLETED"
            }
        },
        "depends_on": [
            {"task_key": previous_task}
        ],
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(status_update_final_task)
    
    return tasks
