import os
import yaml
from datetime import datetime
from typing import List, Dict

def create_workflow_yaml(esp_job_id: str, parent_info: dict, child_jobs: List[dict], project_root_path: str) -> str:
    """
    Create Databricks workflow YAML handling both linear and complex dependencies
    """
    try:
        # Create resources directory
        resources_dir = os.path.join(project_root_path, "resources")
        os.makedirs(resources_dir, exist_ok=True)

        # Generate workflow name
        workflow_name = f"{esp_job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        job_yaml_path = os.path.join(resources_dir, f"{workflow_name}_job.yml")

        # Initialize job configuration
        job_config = {
            "resources": {
                "jobs": {
                    workflow_name: {
                        "name": workflow_name,
                        "tasks": []
                    }
                }
            }
        }

        # Add batch check task
        batch_check_task = _create_batch_check_task()
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(batch_check_task)

        # Add status update task
        status_task = _create_status_task("IN_PROGRESS", ["batch_check"])
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(status_task)

        # Process child jobs in two passes
        task_registry = {}  # task_key -> task_config
        cluster_registry = set()
        job_clusters = []

        # First pass: Create all tasks and clusters
        for child in child_jobs:
            task_config = _create_child_task(child, parent_info.get("job_params", {}))
            
            # Register clusters
            if "cluster_info" in child:
                cluster_config = _process_cluster(child["cluster_info"], cluster_registry)
                if cluster_config:
                    job_clusters.append(cluster_config)
                    task_config["job_cluster_key"] = cluster_config["job_cluster_key"]
            
            task_registry[child["task_name"]] = task_config
            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)

        # Second pass: Process dependencies
        all_dependencies = set()
        for child in child_jobs:
            task_key = child["task_name"]
            task_config = task_registry.get(task_key)
            
            if task_config:
                # Parse and validate dependencies
                dependencies = _parse_dependencies(
                    child.get("depends_on", []),
                    task_registry,
                    default_deps=["status_update_in_progress"]
                )
                
                if dependencies:
                    task_config["depends_on"] = dependencies
                    all_dependencies.update(dependencies)

        # Add final status task
        final_task = _create_final_status_task(task_registry, all_dependencies)
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(final_task)

        # Add job clusters if any
        if job_clusters:
            job_config["resources"]["jobs"][workflow_name]["job_clusters"] = job_clusters

        # Write YAML file
        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)

        return job_yaml_path

    except Exception as e:
        print(f"Error creating workflow: {str(e)}")
        raise

def _create_batch_check_task() -> dict:
    return {
        "task_key": "batch_check",
        "description": "Check for open batch",
        "notebook_task": {
            "notebook_path": "notebooks/fetch_open_batch",
            "source": "GIT"
        },
        "timeout_seconds": 120
    }

def _create_status_task(status: str, dependencies: List[str]) -> dict:
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

def _create_child_task(child: dict, job_params: dict) -> dict:
    """Create individual task configuration"""
    # Handle parameters
    base_params = {
        k: v for k, v in child.get("task_params", {}).items() 
        if k not in job_params
    }
    
    # Handle runner notebook
    if child.get("use_runner"):
        notebook_path = "notebooks/runner_main"
        base_params["notebook_path"] = child["notebook_path"]
    else:
        notebook_path = child["notebook_path"]

    # Build task config
    task_config = {
        "task_key": child["task_name"],
        "description": f"Execute: {child['task_name']}",
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "GIT",
            "base_parameters": base_params
        },
        "timeout_seconds": 7200
    }

    # Add libraries
    if child.get("libraries"):
        task_config["libraries"] = [
            {"whl": lib} if lib.endswith(".whl") else {"jar": lib}
            for lib in child["libraries"]
        ]

    return task_config

def _process_cluster(cluster_info: dict, registry: set) -> dict:
    """Process cluster configuration with deduplication"""
    cluster_key = cluster_info.get("job_cluster_key")
    if cluster_key and cluster_key not in registry:
        registry.add(cluster_key)
        return {
            "job_cluster_key": cluster_key,
            "new_cluster": cluster_info["new_cluster"]
        }
    return None

def _parse_dependencies(raw_deps, task_registry, default_deps=None) -> List[dict]:
    """Parse and validate dependencies from various input formats"""
    # Handle empty dependencies
    if not raw_deps:
        return [{"task_key": dep} for dep in default_deps] if default_deps else []

    # Convert string to list if needed
    if isinstance(raw_deps, str):
        raw_deps = raw_deps.strip("[]").replace("'", "").split(",")
        raw_deps = [d.strip() for d in raw_deps if d.strip()]

    # Validate dependencies exist
    valid_deps = []
    for dep in raw_deps:
        if dep in task_registry or dep in ["batch_check", "status_update_in_progress"]:
            valid_deps.append({"task_key": dep})
    
    return valid_deps if valid_deps else [{"task_key": dep} for dep in default_deps]

def _create_final_status_task(task_registry: dict, all_dependencies: set) -> dict:
    """Create final status task depending on all leaf nodes"""
    # Find leaf tasks (tasks not depended on by others)
    all_tasks = set(task_registry.keys())
    leaf_tasks = all_tasks - all_dependencies
    
    # If no leaf tasks found, default to all tasks
    if not leaf_tasks:
        leaf_tasks = all_tasks

    return {
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
