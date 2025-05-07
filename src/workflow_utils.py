import os
import yaml
import json
from datetime import datetime

def create_workflow_yaml(esp_job_id, parent_info, child_jobs, project_root_path=None):
    """Create a Databricks Asset Bundle job YAML based on job details
    
    Args:
        esp_job_id (str): ESP job ID
        parent_info (dict): Parent job details
        child_jobs (list): List of child job configurations
        project_root_path (str, optional): Root path of the project
    """
    try:            
        # Create resources directory if it doesn't exist
        resources_dir = os.path.join(project_root_path, "resources")
        os.makedirs(resources_dir, exist_ok=True)
        
        # Generate unique workflow name with timestamp
        workflow_name = f"{esp_job_id}"
        print(f"parent_info: {parent_info}")
        print(f"child_jobs: {child_jobs}")
        print(f"project_root_path: {project_root_path}")

        print(type(parent_info))
        print(type(child_jobs))
        
        # Path for the job yml file
        job_yaml_path = os.path.join(resources_dir, f"{workflow_name}_job.yml")
        
        print(f"Writing job YAML to {job_yaml_path}")
        
        # Extract job parameters from parent
        job_parameters = []
        for key, value in parent_info.get("job_params", {}).items():
            job_parameters.append({
                "name": key,
                "default": value
            })
        
        print(f"Job parameters: {job_parameters}")

        # Initialize the job config
        job_config = {
            "resources": {
                "jobs": {
                    workflow_name: {
                        "name": workflow_name,
                        "parameters": job_parameters,
                        "tasks": []
                    }
                }
            }
        }
        
        # Check if any child job has cluster details
        job_clusters = []
        cluster_set = set()
        
        for child in child_jobs:
            if child.get("cluster_info") and "job_cluster_key" in child["cluster_info"]:
                cluster_key = child["cluster_info"]["job_cluster_key"]
                
                # Only add the cluster if we haven't seen it before
                if cluster_key not in cluster_set:
                    job_clusters.append(child["cluster_info"])
                    cluster_set.add(cluster_key)
        
        # Add job clusters if any exist
        if job_clusters:
            job_config["resources"]["jobs"][workflow_name]["job_clusters"] = job_clusters

        git_src = {"git_url": "https://github.com/Megha-jfs/walgreens.git",
                    "git_provider": "GITHUB",
                    "git_branch": "main"
                }
         
        job_config["resources"]["jobs"][workflow_name]["git_source"] = git_src
        
        # Add batch check task as first task
        batch_check_task = {
            "task_key": "batch_check",
            "description": "Check for open batch",
            "notebook_task": {
                "notebook_path": "notebooks/fetch_open_batch",
                "source": "GIT",
                "base_parameters": {}
            },
            "timeout_seconds": 120,
            "retry_on_timeout": False
        }
        
        # Add job cluster key if clusters exist
        if job_clusters:
            batch_check_task["job_cluster_key"] = job_clusters[0]["job_cluster_key"]
        
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(batch_check_task)
        
        # Add status update task as second task
        status_update_task = {
            "task_key": "status_update_start",
            "description": "Mark job as running",
            "notebook_task": {
                "notebook_path": "notebooks/update_job_execution_detail",
                "source": "GIT",                
                "base_parameters": {
                    "status": "IN_PROGRESS"
                }
            },
            "depends_on": [
                {"task_key": "batch_check"}
            ],
            "timeout_seconds": 120,
            "retry_on_timeout": False
        }
        
        # Add job cluster key if clusters exist
        if job_clusters:
            status_update_task["job_cluster_key"] = job_clusters[0]["job_cluster_key"]
        
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(status_update_task)
        
        # Add child tasks
        previous_task = "status_update_start"
        for child in child_jobs:
            if not child.get("notebook_path"):
                print(f"No notebook path found for child job {child['job_id']} - skipping")
                continue
            
            task_key = f"execution_{child['job_id']}_{child['invocation_id']}"
            
            task_config = {
                "task_key": task_key,
                "description": f"Execute task for {child['job_id']}",
                "notebook_task": {
                    #"notebook_path": child["notebook_path"],
                    "notebook_path": "notebooks/runner_main",
                    "source": "GIT",                    
                    "base_parameters": child.get("task_params", {})
                },
                "depends_on": [
                    {"task_key": previous_task}
                ],
                "timeout_seconds": 7200,
                "retry_on_timeout": False
            }
            
            # Add job cluster key if specified in child config
            if child.get("cluster_info") and "job_cluster_key" in child["cluster_info"]:
                task_config["job_cluster_key"] = child["cluster_info"]["job_cluster_key"]
            # Otherwise use the first cluster if available
            elif job_clusters:
                task_config["job_cluster_key"] = job_clusters[0]["job_cluster_key"]
            
            # Add libraries if specified
            if child.get("libraries") and len(child["libraries"]) > 0:
                task_config["libraries"] = child["libraries"]
            
            job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)
            previous_task = task_key
        
        # Add completion task
        completion_task = {
            "task_key": "status_update_complete",
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
        
        # Add job cluster key if clusters exist
        if job_clusters:
            completion_task["job_cluster_key"] = job_clusters[0]["job_cluster_key"]
        
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(completion_task)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(job_yaml_path), exist_ok=True)
        
        # Write the job configuration file
        with open(job_yaml_path, "w") as f:
            yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Successfully created workflow job config in {job_yaml_path}")
        return job_yaml_path
    
    except Exception as e:
        print(f"Error creating workflow YAML: {str(e)}")
        return None
