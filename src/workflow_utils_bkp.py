import json
import requests

def create_workflow(databricks_url, token, workflow_name, tasks, proj_id, dept_id, job_id):
    """Create a Databricks workflow with the given tasks"""
    try:
        # Construct API endpoint
        endpoint = f"{databricks_url}/api/2.1/jobs/create"
        
        # Create workflow configuration
        job_config = {
            "name": workflow_name,
            "job_parameters": {
                "env": "dev",
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": str(job_id),
            },
            "email_notifications": {
                "on_failure": []  # Add email addresses if needed
            },
            "tasks": tasks,
            "format": "MULTI_TASK"
        }
        
        # Call the API
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(endpoint, headers=headers, data=json.dumps(job_config))
        response_data = response.json()
        
        if response.status_code == 200:
            wf_id = response_data.get("job_id")
            print(f"Successfully created workflow with job_id: {wf_id}")
            return wf_id
        else:
            print(f"Failed to create workflow: {response_data}")
            return None
    except Exception as e:
        print(f"Error creating workflow: {str(e)}")
        return None

def build_workflow_tasks(proj_id, dept_id, job_id, child_jobs, notebook_paths):
    """Build tasks for the workflow including batch check, status update, and SQL tasks"""
    tasks = []
    
    # Task 1: Batch check
    fetch_open_batch = {
        "task_key": "fetch_open_batch",
        "description": "Check for open batch",
        "notebook_task": {
            "notebook_path": "/Workspace/Users/megha.upadhyay@walgreens.com/ProcessCtrlFramework/fetch_open_batch",
            "base_parameters": {
                "env": "dev",
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
            }
        },
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(fetch_open_batch)

    # Task 2: Job run check
    check_job_execution = {
        "task_key": "check_job_execution",
        "description": "Check if job already run as part of current batch",
        "notebook_task": {
            "notebook_path": "/Workspace/Users/megha.upadhyay@walgreens.com/ProcessCtrlFramework/check_job_execution",
            "base_parameters": {
                "env": "dev",
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
            }
        },
        "depends_on": [
            {"task_key": "fetch_open_batch"}
        ],       
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(check_job_execution)
    
    # Task 3: Status update (mark as running)
    update_job_execution_detail_inprogress = {
        "task_key": "update_job_execution_detail_inprogress",
        "description": "Mark job as running",
        "notebook_task": {
            "notebook_path": "/Workspace/Users/megha.upadhyay@walgreens.com/ProcessCtrlFramework/update_job_execution_detail",
            "base_parameters": {
                "env": "dev",
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
                "status": "IN_PROGRESS",
                "error_message": ""
            }
        },
        "depends_on": [
            {"task_key": "check_job_execution"}
        ],
        "timeout_seconds": 120,
        "retry_on_timeout": False
    }
    tasks.append(update_job_execution_detail_inprogress)
    
    # Tasks 4+: Notebook execution tasks
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
                "notebook_path": notebook_path,
                "base_parameters": {
                    "proj_id": str(proj_id),
                    "dept_id": str(dept_id),
                    "job_id": child_name,
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
            "notebook_path": "/Workspace/Users/megha.upadhyay@walgreens.com/ProcessCtrlFramework/update_job_execution_detail",
            "base_parameters": {
                "env": "dev",
                "proj_id": str(proj_id),
                "dept_id": str(dept_id),
                "job_id": job_id,
                "status": "IN_PROGRESS",
                "error_message": ""
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
