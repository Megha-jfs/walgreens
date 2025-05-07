import os
import yaml
import json
from datetime import datetime

def create_workflow_yaml(esp_job_id: str, parent_info: dict, child_jobs: list, project_root_path: str) -> str:
    """
    Create Databricks workflow YAML with complex dependencies including parallel paths.
    """
    try:
        # Create resources directory
        resources_dir = os.path.join(project_root_path, "resources")
        os.makedirs(resources_dir, exist_ok=True)

        # Generate workflow name
        workflow_name = f"{esp_job_id}"
        job_yaml_path = os.path.join(resources_dir, f"{workflow_name}_job.yml")

        # Initialize job configuration
        job_config = {
            "resources": {
                "jobs": {
                    workflow_name: {
                        "name": workflow_name,
                        "git_source": {
                            "git_url": "https://github.com/Megha-jfs/walgreens.git",
                            "git_provider": "GITHUB",
                            "git_branch": "main"
                        },
                        "parameters": [],
                        "tasks": []
                    }
                }
            }
        }

        # Add job parameters from parent info (only at job level)
        if parent_info.get("job_params"):
            job_params = parent_info["job_params"]
            if isinstance(job_params, dict):
                job_config["resources"]["jobs"][workflow_name]["parameters"] = [
                    {"name": k, "default": v} for k, v in job_params.items()
                ]
        else:
            job_params = {}

        # Add batch check task
        batch_check_task = {
            "task_key": "batch_check",
            "description": "Check for open batch",
            "notebook_task": {
                "notebook_path": "notebooks/fetch_open_batch",
                "source": "GIT"
            },
            "timeout_seconds": 120
        }
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(batch_check_task)

        # Add status update task
        status_task = {
            "task_key": "status_update_in_progress",
            "description": "Mark job as IN_PROGRESS",
            "notebook_task": {
                "notebook_path": "notebooks/update_job_execution_detail",
                "source": "GIT",
                "base_parameters": {"status": "IN_PROGRESS"}
            },
            "depends_on": [{"task_key": "batch_check"}],
            "timeout_seconds": 120
        }
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(status_task)

        # Process child jobs
        job_clusters = []
        seen_clusters = set()
        task_mapping = {}  # To track task keys for validation
        
        # First pass: Create all tasks without dependencies
        for child in child_jobs:
            # Get set of job-level parameter keys
            job_param_keys = set(job_params.keys())
            # Only include task-specific parameters (exclude job params)
            base_params = {k: v for k, v in child["task_params"].items() if k not in job_param_keys}

            # For runner_main, always add notebook_path
            if child["use_runner"]:
                notebook_path = "notebooks/runner_main"
                base_params["notebook_path"] = child["notebook_path"]
            else:
                notebook_path = child["notebook_path"]

            # Create task configuration
            task_config = {
                "task_key": child["task_name"],
                "description": f"Execute task: {child['task_name']}",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "GIT",
                    "base_parameters": base_params
                },
                "timeout_seconds": 7200
            }

            # Add cluster configuration
            if child.get("cluster_info") and "job_cluster_key" in child["cluster_info"]:
                cluster_key = child["cluster_info"]["job_cluster_key"]
                if cluster_key not in seen_clusters:
                    job_clusters.append(child["cluster_info"])
                    seen_clusters.add(cluster_key)
                task_config["job_cluster_key"] = cluster_key

            # Add libraries if specified
            if child.get("libraries"):
                task_config["libraries"] = [
                    {"jar": lib} if lib.startswith("dbfs:") else {"pypi": {"package": lib}}
                    for lib in child["libraries"]
                ]

            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)
            task_mapping[child["task_name"]] = task_config
        
        # Second pass: Process dependencies correctly
        for child in child_jobs:
            task_key = child["task_name"]
            task = next((t for t in job_config["resources"]["jobs"][workflow_name]["tasks"] 
                        if t.get("task_key") == task_key), None)
            
            if task:
                # Get dependencies
                depends_on = child.get("depends_on", [])
                
                # Parse dependencies if string format
                if isinstance(depends_on, str):
                    # Handle different string formats:
                    depends_on = depends_on.strip("[]").replace("'", "").replace('"', "")
                    depends_on = [d.strip() for d in depends_on.split(",") if d.strip()]
                
                # If task has dependencies specified, use them
                if depends_on:
                    # Filter to only include valid task keys
                    valid_deps = [dep for dep in depends_on 
                                if dep in task_mapping or dep in ["batch_check", "status_update_in_progress"]]
                    if valid_deps:
                        task["depends_on"] = [{"task_key": dep} for dep in valid_deps]
                    else:
                        # Default to status_update_in_progress if no valid dependencies
                        task["depends_on"] = [{"task_key": "status_update_in_progress"}]
                else:
                    # Default to status_update_in_progress for tasks with no deps
                    task["depends_on"] = [{"task_key": "status_update_in_progress"}]
        
        # Find leaf tasks (tasks that no other task depends on)
        all_dependencies = set()
        for task in job_config["resources"]["jobs"][workflow_name]["tasks"]:
            if "depends_on" in task:
                all_dependencies.update([dep["task_key"] for dep in task["depends_on"]])
        
        # Tasks that are not dependencies of any other task are leaf tasks
        leaf_tasks = [task["task_key"] for task in job_config["resources"]["jobs"][workflow_name]["tasks"] 
                    if task["task_key"] not in all_dependencies 
                    and task["task_key"] not in ["batch_check", "status_update_in_progress", "status_update_completed"]]
        
        # If no leaf tasks found, default to all script tasks
        if not leaf_tasks:
            leaf_tasks = [task["task_key"] for task in job_config["resources"]["jobs"][workflow_name]["tasks"]
                        if task["task_key"] not in ["batch_check", "status_update_in_progress", "status_update_completed"]]
        
        # Add final status update task that depends on leaf tasks
        final_status_task = {
            "task_key": "status_update_completed",
            "description": "Mark job as COMPLETED",
            "notebook_task": {
                "notebook_path": "notebooks/update_job_execution_detail",
                "source": "GIT",
                "base_parameters": {"status": "COMPLETED"}
            },
            "depends_on": [{"task_key": task} for task in leaf_tasks],
            "timeout_seconds": 120
        }
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(final_status_task)

        # Add job clusters if any
        if job_clusters:
            job_config["resources"]["jobs"][workflow_name]["job_clusters"] = job_clusters

        # Write YAML file
        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)

        print(f"Successfully created workflow YAML at {job_yaml_path}")
        return job_yaml_path

    except Exception as e:
        print(f"Error creating workflow YAML: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
