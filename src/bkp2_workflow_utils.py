import os
import yaml
import json
from datetime import datetime
from typing import Dict, List, Optional

def create_workflow_yaml(esp_job_id: str, parent_info: Dict, child_jobs: List[Dict], project_root_path: str) -> str:
    """Create Databricks workflow YAML with dynamic configurations"""
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
                        "tasks": []
                    }
                }
            }
        }

        # Process clusters and libraries
        job_clusters = []
        seen_clusters = set()

        # Add batch check task
        batch_check_task = _create_batch_check_task()
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(batch_check_task)

        # Add status update task
        status_task = _create_status_task("IN_PROGRESS", ["batch_check"])
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(status_task)

        # Process child jobs
        task_mapping = {}
        for child in child_jobs:
            task_config = _process_child_job(child, job_clusters, seen_clusters)
            task_mapping[child["task_name"]] = task_config
            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)

        # Add final status task
        final_status_task = _create_status_task("COMPLETED", [child_jobs[-1]["task_name"]])
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(final_status_task)

        # Add clusters to job config
        if job_clusters:
            job_config["resources"]["jobs"][workflow_name]["job_clusters"] = job_clusters

        # Write YAML file
        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)

        return job_yaml_path

    except Exception as e:
        print(f"Error creating workflow YAML: {str(e)}")
        raise

def _create_batch_check_task() -> Dict:
    """Create batch check task configuration"""
    return {
        "task_key": "batch_check",
        "description": "Check for open batch",
        "notebook_task": {
            "notebook_path": "notebooks/fetch_open_batch",
            "source": "GIT"
        },
        "timeout_seconds": 120
    }

def _create_status_task(status: str, dependencies: List[str]) -> Dict:
    """Create status update task configuration"""
    return {
        "task_key": f"status_update_{status.lower()}",
        "description": f"Mark job as {status}",
        "notebook_task": {
            "notebook_path": "notebooks/update_job_execution_detail",
            "source": "GIT",
            "base_parameters": {"status": status}
        },
        "depends_on": [{"task_key": dep} for dep in dependencies],
        "timeout_seconds": 120
    }

def _process_child_job(child: Dict, job_clusters: List, seen_clusters: set) -> Dict:
    """Process individual child job configuration"""
    # Handle notebook path and parameters
    if child["use_runner"]:
        notebook_path = "notebooks/runner_main"
        base_params = {**child["task_params"], "notebook_path": child["notebook_path"]}
    else:
        notebook_path = child["notebook_path"]
        base_params = child["task_params"]

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

    # Add dependencies
    depends_on = child.get("depends_on", [])

    if not depends_on and child.get("invocation_id") == 1:
        depends_on = ["status_update_in_progress"]
    elif depends_on:
        if isinstance(depends_on, str):
            depends_on = [d.strip() for d in depends_on.strip("[]").split(",")]
    
    task_config["depends_on"] = [{"task_key": dep} for dep in depends_on]

    # Add cluster configuration
    if child.get("cluster_info"):
        cluster_key = child["cluster_info"]["job_cluster_key"]
        if cluster_key not in seen_clusters:
            job_clusters.append(child["cluster_info"])
            seen_clusters.add(cluster_key)
        task_config["job_cluster_key"] = cluster_key

    # Add libraries
    if child.get("libraries"):
        task_config["libraries"] = [_parse_library(lib) for lib in child["libraries"]]

    return task_config

def _parse_library(lib: str) -> Dict:
    """Parse library string into Databricks YAML format"""
    if lib.startswith("dbfs:") or lib.startswith("/Workspace"):
        return {"whl": lib}
    if lib.startswith("jar:"):
        return {"jar": lib.split("jar:")[1]}
    if "@" in lib:  # PyPI package
        return {"pypi": {"package": lib}}
    return {"maven": {"coordinates": lib}}
