- task_key: jdbc_import_outer_loop
          depends_on:
            - task_key: jdbc_import_task
          for_each_task:
            inputs: "{{tasks.jdbc_import_task.values.tables}}"
            concurrency: 15
            task:
              task_key: jdbc_import_outer_loop_iteration
              notebook_task:
                notebook_path: /Workspace/Users/shad.khan1@walgreens.com/dlx-databricks-jdbc/src/utility/PCF/runner_main
                base_parameters:
                  source_table: "{{input}}"
                  notebook_path: /src/utility/JDBC/JDBC Framework/JDBC Framework/Trunc and Load
                  child_job_id: jdbc_import_outer_loop_iteration_"{{input}}"
                source: WORKSPACE
              existing_cluster_id: 0529-223445-2mf397j8
