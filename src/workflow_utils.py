import os
import yaml
from datetime import datetime

def create_workflow_bundle(workflow_name, tasks, job_parameters=None, project_root_path=None):
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

        resources_dir = os.path.join(project_root_path, "resources")
        # os.makedirs(resources_dir, exist_ok=True)
        print(f"Created directory structure at {resources_dir}")

        job_yaml_path = os.path.join(resources_dir, f"{workflow_name}_job.yml")
        print(f"Writing job config to {job_yaml_path}")
        
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
                                    "spark_version": "15.4.x-scala2.12",
                                    " azure_attributes": {
                                        "first_on_demand": 1,
                                        "availability": "SPOT_WITH_FALLBACK_AZURE",
                                        "spot_bid_max_price": "-1"
                                    },
                                    "node_type_id": "Standard_D4ds_v5",
                                    "spark_env_vars": {
                                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                                    },
                                    "enable_elastic_disk": "true",
                                    "data_security_mode": "USER_ISOLATION",
                                    "runtime_engine": "STANDARD",
                                    "autoscale": {
                                        "min_workers": 2,
                                        "max_workers": 4
                                    }
                                }
                            }
                        ],
                        "git_source": {
                            "git_url": "https://github.com/Megha-jfs/walgreens.git",
                            "git_provider": "GITHUB",
                            "git_branch": "main"
                        },
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
        print("going to write yaml file")
        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Successfully created workflow job config in {resources_dir}/{workflow_name}_job.yml")
        print(f"Note: Use this with your manually created databricks.yml file")
        
        return True
    except Exception as e:
        print(f"Error creating workflow bundle: {str(e)}")
        return None

def build_workflow_tasks(proj_id, dept_id, job_id, child_jobs, notebook_paths):
    """Build tasks for the workflow including batch check, status update, and SQL tasks"""
    tasks = []
    
    # Task 1: Batch check
    fetch_open_batch = {
        "task_key": "fetch_open_batch",
        "description": "Check for open batch",
        "notebook_task": {
            "notebook_path": "notebooks/fetch_open_batch",
            "source": "GIT"
        },
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(fetch_open_batch)
    
    # Task 2: Status update (mark as inprogress)
    update_job_execution_detail_inprogress = {
        "task_key": "update_job_execution_detail_inprogress",
        "description": "Mark job as inprogress",
        "notebook_task": {
            "notebook_path": "notebooks/update_job_execution_detail",
            "source": "GIT",
            "base_parameters": {
                # "proj_id": str(proj_id),
                # "dept_id": str(dept_id),
                # "job_id": job_id,
                "status": "IN_PROGRESS"
            }
        },
        "depends_on": [
            {"task_key": "fetch_open_batch"}
        ],
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(update_job_execution_detail_inprogress)
    
    # Tasks 3+: SQL execution tasks
    previous_task = "update_job_execution_detail_inprogress"
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
                "notebook_path": "notebooks/runner_main",
                "source": "GIT",
                "base_parameters": {
                    "notebook_path": notebook_path
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
    update_job_execution_detail_completed = {
        "task_key": "update_job_execution_detail_completed",
        "description": "Mark job as completed",
        "notebook_task": {
            "notebook_path": "notebooks/update_job_execution_detail",
            "source": "GIT",
            "base_parameters": {
                "status": "COMPLETED"
            }
        },
        "depends_on": [
            {"task_key": previous_task}
        ],
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(update_job_execution_detail_completed)
    
    return tasks
