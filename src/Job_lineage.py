# Databricks notebook source
import requests
import json
from collections import defaultdict, deque
from typing import Dict, List, Set, Optional, Tuple, Union

# COMMAND ----------

access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
lineage_endpoint = f"{workspace_url}/api/2.0/lineage-tracking/table-lineage"
headers = {
"Authorization": f"Bearer {access_token}",
"Content-Type": "application/json"
}

# COMMAND ----------

def get_table_full_upstream_lineage(table_name: str) -> Dict:
        """
        Get the upstream lineage for a specific table using Databricks REST API with different behavior
        based on schema types:
        - For *tb schemas: Only find immediate parent tables (one level)
        - For *wt schemas: Find all associated tables in the complete lineage
        
        Args:
            table_name (str): The fully qualified table name (catalog.schema.table)
            final_table (str, optional): A stopping point table name if provided
            
        Returns:
            dict: A dictionary containing the filtered upstream lineage information
        """
        # Complete lineage result structure
        result = {
            "root_table": table_name,
            "all_upstream_tables": set(),  # Use set to avoid duplicates
            "lineage_graph": {},           # Will store the parent-child relationships
            "details": {}                  # Table details
        }
        
        # Use a queue for breadth-first traversal and a set to track visited tables
        tables_to_process = deque([(table_name, 0)])  # (table_name, depth)
        visited_tables = set()
        print("tables_to_process", tables_to_process)
        # Process tables breadth-first to build complete lineage
        while tables_to_process:
            current_table, depth = tables_to_process.popleft()
            
            # Skip if already visited to prevent cycles
            if current_table in visited_tables:
                continue
            
            visited_tables.add(current_table)
            
            try:
                # Make the API request for the current table
                response = requests.get(
                    lineage_endpoint, 
                    headers=headers, 
                    json={"table_name": current_table, "include_entity_lineage": True},
                    timeout=10
                )
                if response.status_code != 200:
                    # Log error but continue processing other tables
                    result["lineage_graph"][current_table] = {
                        "error": f"API returned status code {response.status_code}"
                    }
                    continue
                
                data = response.json()
                # Extract the schema name of the current table
                current_table_parts = current_table.split('.')
                current_schema = current_table_parts[1] if len(current_table_parts) >= 3 else ""
                
                # Initialize the entry in the lineage graph
                if current_table not in result["lineage_graph"]:
                    result["lineage_graph"][current_table] = {"upstream_tables": []}
                
                # Process upstream tables
                if "upstreams" in data:
                    for upstream in data["upstreams"]:
                        if "tableInfo" in upstream:
                            info = upstream["tableInfo"]
                            upstream_name = f"{info['catalog_name']}.{info['schema_name']}.{info['name']}"
                            
                            # Add to the set of all upstream tables
                            result["all_upstream_tables"].add(upstream_name)
                            
                            # Add to the current table's upstream list in the graph
                            result["lineage_graph"][current_table]["upstream_tables"].append(upstream_name)
                            
                            # Store details
                            result["details"][upstream_name] = {
                                "name": info["name"],
                                "catalog": info["catalog_name"],
                                "schema": info["schema_name"]
                            }
                            
                            # Add table type if available
                            if "table_type" in info:
                                result["details"][upstream_name]["table_type"] = info["table_type"]
                            
                            tables_to_process.append((upstream_name, depth + 1))
                                
                            # # Check if we should continue processing based on schema type
                            # # For *tb schemas, only process the first level (depth=0)
                            # # For *wt schemas, process all levels
                            # if upstream_name not in visited_tables:
                            #     is_tb_schema = "_tb" in info['schema_name'].lower()
                            #     is_wt_schema = "_wt" in info['schema_name'].lower()
                                
                            #     # For tb schemas, only add if we're at depth 0 (immediate parents)
                            #     # For wt schemas, add regardless of depth for full lineage
                            #     if is_wt_schema or (is_tb_schema and depth == 0):
                            #         tables_to_process.append((upstream_name, depth + 1))
                    
            except Exception as e:
                # Log error but continue processing other tables
                result["lineage_graph"][current_table] = {
                    "error": str(e)
                }
        
        # Convert set to list for better JSON serialization
        result["all_upstream_tables"] = list(result["all_upstream_tables"])
        
        return result

# COMMAND ----------

def get_databricks_credentials():
    try:
        # Get workspace URL
        workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
        workspace_url = f"https://{workspace_url}"
        
        # Get access token - multiple methods to try
        access_token = None
        
        # Method 1: Try to get from secrets scope
        try:
            access_token = dbutils.secrets.get(scope="databricks", key="access_token")
        except:
            pass
        
        # Method 2: Try to get from notebook context (for personal access tokens)
        if not access_token:
            try:
                access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            except:
                pass
        
        # Method 3: Use current session token
        if not access_token:
            try:
                access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
            except:
                pass
        
        return workspace_url, access_token
        
    except Exception as e:
        print(f"Error getting Databricks credentials: {e}")
        raise

def get_job_run_details(run_id: str) -> dict:
    """
    Get the details of a specific job run using the Databricks Jobs Runs Get API.
    
    Args:
        run_id (str): The run ID to retrieve details for.
        
    Returns:
        dict: A dictionary containing the job run details.
    """
    databricks_instance, token = get_databricks_credentials()
    url = f"{databricks_instance}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.text}

# COMMAND ----------

def get_table_job_lineage(x: str):
    # Initialize an empty list to store the table and job lineage information
    table_job_lineage = []
    # Get the full upstream lineage for the specified table
    lineage_result = get_table_full_upstream_lineage(x)

    # Filter the lineage graph to include only tables that have upstream tables
    table_lineage = {k: v for k, v in lineage_result["lineage_graph"].items() if v.get("upstream_tables") != []}
    all_tables = list(table_lineage.keys())
    for upstream in table_lineage.values():
        all_tables.extend(upstream['upstream_tables'])
    all_tables = list(set(all_tables))
    # Iterate over each table and its lineage information
    for table_name in all_tables:
        # Print the table name
        print(table_name)
        
        try:
            # Get the latest run ID for the table by querying the table's history
            run_id = spark.sql(f"""
                  select job.runId as run_id from (describe history {table_name}) order by version desc limit 1
                  """
            ).collect()[0][0]
            
            # Get the job run details using the run ID
            job_run_details = get_job_run_details(run_id)
            
            # Extract the esp_job_id from the job run details
            try:
                esp_job_id = job_run_details['tasks'][0]['notebook_task']['base_parameters']['esp_job_id']
            except (KeyError, IndexError) as e:
                esp_job_id = None
            
            # Append the table name and esp_job_id to the table_job_lineage list
            table_job_lineage.append((table_name, esp_job_id))
        
        except Exception as e:
            # Handle any exceptions that occur during the process
            table_job_lineage.append((table_name, None))
            print(f"Error processing table {table_name}: {e}")
    
    return table_job_lineage

# COMMAND ----------

# Define the table name
x = 'data_migration_validator_dev.dae_cooked.ch_reconciliation'.lower()
result=get_table_job_lineage(x)
print(result)


# COMMAND ----------

