import os
import yaml
from datetime import datetime

def create_workflow_yaml(esp_job_id: str, parent_info: dict, child_jobs: list, project_root_path: str) -> str:
    """
    Create Databricks workflow YAML with both linear and complex dependencies.
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

        # --- FIRST PASS: Create all child tasks and build task_mapping ---
        job_clusters = []
        seen_clusters = set()
        task_mapping = {}  # task_name -> task_config

        for child in child_jobs:
            job_param_keys = set(job_params.keys())
            base_params = {k: v for k, v in child["task_params"].items() if k not in job_param_keys}

            if child["use_runner"]:
                notebook_path = "notebooks/runner_main"
                base_params["notebook_path"] = child["notebook_path"]
            else:
                notebook_path = child["notebook_path"]

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

            if child.get("cluster_info") and "job_cluster_key" in child["cluster_info"]:
                cluster_key = child["cluster_info"]["job_cluster_key"]
                if cluster_key not in seen_clusters:
                    job_clusters.append(child["cluster_info"])
                    seen_clusters.add(cluster_key)
                task_config["job_cluster_key"] = cluster_key

            if child.get("libraries"):
                task_config["libraries"] = [
                    {"jar": lib} if lib.startswith("dbfs:") else {"pypi": {"package": lib}}
                    for lib in child["libraries"]
                ]

            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)
            task_mapping[child["task_name"]] = task_config

        # --- SECOND PASS: Add dependencies to each child task ---
        all_dependencies = set()
        for child in child_jobs:
            task_key = child["task_name"]
            task = task_mapping.get(task_key)
            if not task:
                continue

            depends_on = child.get("depends_on", [])
            # Parse dependencies if string format
            if isinstance(depends_on, str):
                depends_on = depends_on.strip("[]").replace("'", "").replace('"', "")
                depends_on = [d.strip() for d in depends_on.split(",") if d.strip()]

            # Convert all dependencies to strings (if dict, extract task_key)
            processed_deps = []
            for dep in depends_on:
                if isinstance(dep, dict):
                    processed_deps.append(dep.get("task_key", ""))
                else:
                    processed_deps.append(str(dep))

            # Filter valid dependencies
            valid_deps = [
                dep for dep in processed_deps
                if dep and (dep in task_mapping or dep in ["batch_check", "status_update_in_progress"])
            ]

            if valid_deps:
                task["depends_on"] = [{"task_key": dep} for dep in valid_deps]
                all_dependencies.update(valid_deps)
            else:
                # Default to status_update_in_progress for tasks with no deps
                task["depends_on"] = [{"task_key": "status_update_in_progress"}]
                all_dependencies.add("status_update_in_progress")

        # --- Find leaf tasks (tasks not depended on by any other task) ---
        all_task_keys = set(task_mapping.keys())
        leaf_tasks = all_task_keys - all_dependencies
        if not leaf_tasks:
            # fallback: all child tasks
            leaf_tasks = all_task_keys

        # --- Add final status update task that depends on leaf tasks ---
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

        if job_clusters:
            job_config["resources"]["jobs"][workflow_name]["job_clusters"] = job_clusters

        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)

        print(f"Successfully created workflow YAML at {job_yaml_path}")
        return job_yaml_path

    except Exception as e:
        print(f"Error creating workflow YAML: {str(e)}")
        return None
