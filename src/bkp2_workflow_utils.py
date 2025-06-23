for child in child_jobs:
    # ... existing logic to create task_config ...
    job_config["resources"]["jobs"][workflow_name]["tasks"].append(task_config)
    task_mapping[child["task_name"]] = task_config

    # Enhancement: Add jdbc_import_outer_loop if this is jdbc_import_task
    if child["task_name"] == "jdbc_import_task":
        jdbc_outer_loop_task = {
            "task_key": "jdbc_import_outer_loop",
            "depends_on": [{"task_key": "jdbc_import_task"}],
            "for_each_task": {
                "inputs": "{{tasks.jdbc_import_task.values.tables}}",
                "concurrency": 15,
                "task": {
                    "task_key": "jdbc_import_outer_loop_iteration",
                    "notebook_task": {
                        "notebook_path": "src/utility/PCF/runner_main",
                        "base_parameters": {
                            "source_table": "{{input}}",
                            "notebook_path": "/src/utility/JDBC/JDBC Framework/JDBC Framework/Trunc and Load",
                            "child_job_id": "jdbc_import_outer_loop_iteration_{{input}}"
                        },
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": default_cluster_key if default_cluster_key else None
                }
            }
        }
        # Clean up None fields
        if jdbc_outer_loop_task["for_each_task"]["task"]["job_cluster_key"] is None:
            del jdbc_outer_loop_task["for_each_task"]["task"]["job_cluster_key"]
        job_config["resources"]["jobs"][workflow_name]["tasks"].append(jdbc_outer_loop_task)
        task_mapping["jdbc_import_outer_loop"] = jdbc_outer_loop_task
