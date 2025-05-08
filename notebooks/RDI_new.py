# Databricks notebook source
# MAGIC %run ./logger_utils

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import json, re, datetime, traceback

class IngestionManager:
    def __init__(self, project_id, dept_id, job_id,workflow_id,run_id ,logger=None):
        self.project_id = project_id
        self.dept_id = dept_id
        self.job_id = job_id
        self.workflow_id = workflow_id
        self.job_id = job_id
        self.run_id = run_id
        self.config = self._get_config(proj_id, dept_id, job_id)


    def _get_config(self,proj_id, dept_id, job_id):
        config_df = spark.table("data_migration_validator_dev.idh_prod_analysis.RDI_config_table") \
            .filter((F.col("project_id") == self.project_id) &
                    (F.col("dept_id") == self.dept_id) &
                    (F.col("job_id") == self.job_id))
        duplicate_entities = config_df.groupBy("entity_name").count().filter(F.col("count") > 1).select("entity_name").collect()
        if duplicate_entities:
            raise Exception(f"Duplicate entries found for entity_names: {[row.entity_name for row in duplicate_entities]}")
        return config_df.select(
            F.col("entity_name"),
            F.struct(*[F.col(c) for c in config_df.columns]).alias("config_elements")
        ).collect()


  

    def _save_raw_data_to_table(self, source_path, catalog_name, schema_name, table_name, transformations_json, skiplines):
        # Read the data from the source 
        self.logger.info(f"_save_raw_data_to_table(): Started ")
        raw_data_df = spark.read.option("skipRows", skiplines).text(source_path)

        # Select all columns and combine them into a single column called 'raw_data'
        raw_data_combined_df = raw_data_df.selectExpr("concat_ws(',', *) as raw_data")

        # Add the current date as a partition column
        result_df = self._apply_transformations(raw_data_combined_df, transformations_json)

        # Save to a table (using overwrite or append based on your requirement)
        result_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("PartitionColumn") \
            .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")

        if self.logger: self.logger.info(f"save_raw_data_to_table() :Raw Data saved to table '{catalog_name}.{schema_name}.{table_name}'")





    def _generate_select_expr_from_fixed_width(self, position_str, schema_str, input_column="input_column"):
            """
            Generates Spark selectExpr-compatible substring expressions for fixed-width files.

            Parameters:
            - position_str (str): e.g. '1-5,6-11,12-25,...'
            - schema_str (str): e.g. 'str_nbr:chararray,wic_nbr:chararray,...'
            - input_column (str): name of the source column (default = 'input_column')
            []
            Returns:
            - List of expressions for use with df.selectExpr() --[substring({input_column}, {start}, {length}),substring({input_column}, {start}, {length})]
            """
            self.logger.info(f"generate_select_expr_from_fixed_width(): Started ")
            positions = position_str.strip().split(',')
            #columns = [col.split(':')[0].strip() for col in schema_str.strip().split(',')]
            columns =  schema_str
            if len(positions) != len(columns):
                self.logger.error(f"Mismatch between number of position ranges {len(positions)} and columns {len(columns)} ")
                raise ValueError("Mismatch between number of position ranges {len(positions)} and columns {len(columns)} ")

            expressions = []
            for pos, col in zip(positions, columns):
                start, end = map(int, pos.split('-'))
                length = end - start + 1
                expressions.append(f"substring({input_column}, {start}, {length}) as {col}")
            if self.logger: self.logger.info(f"generate_select_expr_from_fixed_width() : Expressions generated -- {expressions}")
            return expressions
        


    def _load_file_into_dataframe(self, item):
            """
            Loads raw data from a specified source path into a Spark DataFrame.

            This function supports both delimited and fixed-width file formats.
            It:
            - Reads configuration values such as path, delimiter, header info, and skip lines.
            - Handles special delimiters like 'Ç'.
            - Processes fixed-width files using positional metadata.
            - Validates column count against the expected header.
            - Applies headers if configured to do so.
            - Returns the processed DataFrame, the original header string, and row count.

            Parameters:
                item: An object containing configuration parameters (like file path, format, header, etc.)

            Returns:
                df (DataFrame): Spark DataFrame containing the processed data.
                header_string (str): Original header string from config.
                file_count (int): Number of rows loaded into the DataFrame.
            """
            delimiter=item.config_elements.Delimiter
            path=item.config_elements.Src_Path+item.config_elements.File_Pattern
            header_flag=item.config_elements.Header_Flag
            header_string=item.config_elements.Header
            skiplines=item.config_elements.Skiplines
            position=item.config_elements.Fixed_Length_Position
            header = [h.strip() for h in header_string.split(",")]

            if delimiter =='Ç':
                delimiter='\u00C7'
            if (delimiter =='\x01' or delimiter =='x01'): 
                delimiter='\u0001'
            
            #print(delimiter)
            if delimiter =='':
                raw_data_df = spark.read.option("skipRows", skiplines).text(path)
                raw_data_combined_df = raw_data_df.selectExpr("concat_ws(',', *) as raw_data")
                exprs = self._generate_select_expr_from_fixed_width(position, header,'raw_data')
                df = raw_data_combined_df.selectExpr(exprs)
            else:
                df = spark.read.option("delimiter", delimiter).option("skipRows", skiplines).csv(path) #

            if self.logger: self.logger.info(f"_load_file_into_dataframe() : Loaded File into a df  with delimiter - {delimiter}  and skipRows - {skiplines}")
            
            result = self._validate_column_count(df, header)
            if header_flag and header and result:
                df = df.toDF(*header) 
            if self.logger: self.logger.info(f"_load_file_into_dataframe() : Applied headers to the df - {header}")
            file_count = df.count()
            df.show() 
            return df,header_string,file_count
        

    def _dq_checks(self,df):

            None



    def _validate_column_count(self,df, expected_columns):
            """
            Checks if the number of columns in the DataFrame matches the expected column list.

            Args:
                df (DataFrame): The Spark DataFrame to validate.
                expected_columns (str or list): Comma-separated string or list of expected column names.

            Returns:
                bool: True if column counts match, False otherwise.
            """
            if isinstance(expected_columns, str):
                expected_columns = [col.strip() for col in expected_columns.split(',')]
            
            actual_count = len(df.columns)
            expected_count = len(expected_columns)

            self.logger.info(f"Actual columns: {actual_count}")
            self.logger.info(f"Expected columns: {expected_count}")

            if actual_count != expected_count:
                self.logger.info("Mismatch! Columns do not match.")
                return False
            else:
                self.logger.info("Column count matches.")
                return True
        
    def _table_exists(self, catalog_name, schema_name, table_name):
            """
            Checks if the specified table exists in the catalog.
            
            Returns:
                True if the table exists, False otherwise.
            """
            try:
                spark.catalog.getTable(f"{catalog_name}.{schema_name}.`{table_name}`")
                return True
            except AnalysisException:
                return False


    
    def _get_table_schema(self, catalog_name, schema_name, table_name):
            try:
                schema_info = spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.{table_name}")
                return {row['col_name']: row['data_type'] for row in schema_info.collect()}
            except AnalysisException as e:
                if "Table or view not found" in str(e):
                    if self.logger: self.logger.info(f"Table not found: {catalog_name}.{schema_name}.{table_name}")
                    return None
                else:
                    raise


    
    def _get_spark_type(self,column_type):
            """Map the SQL data type to a Spark SQL data type."""
            type_mapping = {
                'string': StringType(),
                'int': IntegerType(),
                'float': FloatType(),
                'double': DoubleType(),
                'date': DateType(),
                'timestamp': TimestampType(),
                'boolean': BooleanType(),
            }
            return type_mapping.get(column_type.lower(), column_type.lower())  
        

    def _impose_table_schema(self,df, table_schema):
            """Impose the table schema on the DataFrame, casting columns as per the schema."""
            #df=self.normalize_column_names(df)  
            table_columns=[]
            for column_name, column_type in table_schema.items():
                table_columns.append(column_name)
                if column_name in df.columns:
                    spark_type = self._get_spark_type(column_type)
                    df = df.withColumn(column_name, F.col(column_name).cast(spark_type))
                else :
                    if self.logger: self.logger.info(f"Column {column_name} not found in the File DataFrame but present in Table DDL.")
            if self.logger: self.logger.info(f"Table schema imposed")
            return df


    def _write_to_table(self, df, catalog_name, schema_name, table_name,table_mode,partition_column):
            """Write the final DataFrame to the Databricks table."""
            if self.logger: self.logger.info("Writing to table")
            try:
                table_count_before_writing = spark.table(f"{catalog_name}.{schema_name}.`{table_name}`").count()
            except:
                table_count_before_writing = 0 
            # if len(partition_column) > 0:    
            #     df.write \
            #     .format("delta") \
            #     .mode({table_mode}) \
            #     .partitionBy({partition_column}) \
            #     .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}") 
            # else:
            #      df.write \
            #     .format("delta") \
            #     .mode({table_mode}) \
            #     .saveAsTable(f"{catalog_name}.{schema_name}.{table_name}") 
            writer = df.write.format("delta").mode(table_mode)

            # Add partitionBy only if partition_column is valid
            if partition_column:
                if isinstance(partition_column, str):
                    writer = writer.partitionBy(partition_column)
                elif isinstance(partition_column, list) and all(isinstance(col, str) for col in partition_column):
                    writer = writer.partitionBy(*partition_column)
                else:
                    raise ValueError(f"Invalid partition_column: {partition_column}")

            writer.saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")

            spark.table(f"{catalog_name}.{schema_name}.`{table_name}`")
            table_count_after_writing = spark.table(f"{catalog_name}.{schema_name}.`{table_name}`").count()
            # # Filter out rows where all columns are null
            # non_null_rows = table_count.filter(
            #     ~reduce(lambda a, b: a & b, [F.col(c).isNull() for c in table_count.columns])
            # ).count()
            # print(f"Non-null rows: {non_null_rows}")
            # if non_null_rows == 0:
            #     raise ValueError(f"Table {catalog_name}.{schema_name}.{table_name} is empty. Data not loaded ")
            if self.logger: self.logger.info(f"_write_to_table(): Table {catalog_name}.{schema_name}.{table_name} has been successfully processed and saved ,with mode {table_mode} and partition column {partition_column}")
            return table_count_before_writing,table_count_after_writing
        

 
    def _get_file_list_from_abfss_path(self,path,pattern_str):
            """
            List files from ABFSS path using dbutils and match pattern locally.

            path_pattern: e.g., 'abfss://external@sftpdemostorage01.dfs.core.windows.net/CoreHRTrans/CoreHR_OI_FND_FODivision*.dat'
            """
            if self.logger: self.logger.info(f" get_file_list_from_abfss_path() - started")

            if '?' in pattern_str:
                pattern_str = pattern_str.replace('?', '.')

            # Compile regex pattern (case-sensitive; add re.IGNORECASE if needed)
            regex = re.compile(pattern_str)

            # List files in the directory (non-recursive)
            files = dbutils.fs.ls(path)

            # Filter files matching the regex pattern
            matching_files = [f.name for f in files if regex.fullmatch(f.name)]
            if self.logger: self.logger.info(f" get_file_list_from_abfss_path() - Here are the List of files {matching_files} which are matching the pattern {pattern_str}  at location {path}")
            return matching_files
        
    def _get_source_file_list(self,df) :
            """
            Extracts and returns a list of distinct source file paths from the DataFrame's metadata.

            Parameters:
            df (DataFrame): The Spark DataFrame that includes '_metadata.file_path' column.

            Returns:
            list: A list of unique file paths read into the DataFrame.
            """
            
            if "_metadata.file_path" not in df.columns:
                raise ValueError("The DataFrame does not contain '_metadata.file_path'. Make sure 'includeMetadata' is enabled.")

            file_paths = df.select(F.col("_metadata.file_path")).distinct()
            return [row.file_path for row in file_paths.collect()]




    def _apply_transformations(self,df: DataFrame, transformations_str: str) -> DataFrame:
            """Apply transformation logic to specified columns using a JSON string."""
            try:
                transformations = json.loads(transformations_str)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON format for transformations: {e}")

            for transformation in transformations:
                column_name = transformation["column_name"]
                transformation_logic = transformation["transformation_logic"]
                df = df.withColumn(column_name, F.expr(transformation_logic))
            if self.logger: self.logger.info(f" _apply_transformations() - Transformations applied  {transformations} ")
            return df



                    
    def _log_ingestion_audit(
        self,
        audit_catalog_name: str,
        audit_schema_name: str,
        audit_table_name: str,
        entity_name: str,
        file_names: list,
        table_name: str,
        previous_row_count_in_table: int,
        new_row_count_in_table: int,
        file_row_count: int,
        file_pattern: str,
        status: str,
        source_path: str,
        error_message: str = None
    ):
            """Logs ingestion details to an audit table."""
            run_timestamp = datetime.datetime.now()

            # Calculate rows inserted
            #rows_inserted = 0 #new_row_count_in_table - previous_row_count_in_table

            # Define the schema explicitly
            schema = StructType([
                StructField("processed_timestamp", TimestampType(), False),
                StructField("entity_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("file_names", ArrayType(StringType()), True),
                StructField("file_pattern", StringType(), True),
                StructField("previous_row_count_in_table", IntegerType(), True),
                StructField("new_row_count_in_table", IntegerType(), True),
                #StructField("rows_inserted", IntegerType(), True),
                StructField("file_row_count", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("source_path", StringType(), True),
                StructField("job_id", StringType(), True),
                StructField("run_id", StringType(), True)
            ])

            # Prepare the data row
            audit_row = [(run_timestamp, entity_name, table_name, file_names, file_pattern,
                        previous_row_count_in_table, new_row_count_in_table, 
                        #rows_inserted,
                        file_row_count, status, error_message, source_path, self.workflow_id,self.run_id)]

            audit_df = spark.createDataFrame(audit_row, schema)
            
            # Write to Delta table
            full_table_name = f"{audit_catalog_name}.{audit_schema_name}.{audit_table_name}"
            audit_df.write.mode("append").format("delta").saveAsTable(full_table_name)
            self.logger.info(f"✅ Audit log inserted into {full_table_name}")


    def _move_processed_files_to_archieve(self,source,destination,file_names_to_move):

            files = dbutils.fs.ls(source)
            
            for file in files:
                if file.isFile() and file.name in file_names_to_move:
                    dest_path = destination + file.name
                    dbutils.fs.mv(file.path, dest_path)
                    self.logger.info(f"Moved Processed files from {source} to {destination}: {file.name}")

    def process_ingestion(self):
            #config_list = self._get_config(proj_id, dept_id, job_id)

            for item in self.config:
                catalog_name = item.config_elements.Target_Catalog
                schema_name = item.config_elements.Target_Schema
                table_name = item.config_elements.Entity_Name
                raw_catalog_name = item.config_elements.Raw_Catalog
                raw_schema_name = item.config_elements.Raw_Schema
                audit_catalog_name = item.config_elements.Audit_Catalog
                audit_schema_name = item.config_elements.Audit_Schema
                audit_table_name = item.config_elements.Audit_Table
                path = item.config_elements.Src_Path
                pattern = item.config_elements.File_Pattern
                transformations_json = item.config_elements.Transformations
                skiplines = item.config_elements.Skiplines
                archival_path = item.config_elements.Archival_Path
                log_path = item.config_elements.Log_Path
                raw_ingestion_flag = item.config_elements.Raw_Ingestion_Flag
                table_mode = item.config_elements.Table_Mode
                partition_column=item.config_elements.Partition_Column
                #self.logger = get_logger(f"Logger_{table_name}", table_name )
                if log_path:
                    self.logger = get_logger(f"Logger_{table_name}", log_path,table_name )
                else:
                    self.logger = get_logger(f"Logger_{table_name}", table_name )
          
                

                try:
                    #step1:
                    list_of_files = self._get_file_list_from_abfss_path(path, pattern)
                    
                    
                    if len(list_of_files) != 0:
                        #step2: 
                        if raw_ingestion_flag:
                            self._save_raw_data_to_table(path, raw_catalog_name, raw_schema_name, table_name, transformations_json, skiplines)
                        else:
                            self.logger.info(f"Skipping raw ingestion for {table_name}")
                        

                        ##step3:  Load the data into DataFrame,
                        #         based on the delimiter it process delimiter files and fixed length files
                        #         If Fixed length it checks if the Positions and headers are proper ,if not it raises exception
                        #         If delimiter, validate if the header counts provided in the config file matches with the actual header count
                        df, expected_columns, file_count = self._load_file_into_dataframe(item)

                        if not df.isEmpty():
                        #step4: If there are any extra columns needed / column modified in the config file, apply the transformations
                            if transformations_json!='':
                                df = self._apply_transformations(df, transformations_json)

                            #step5: Check if the table exists
                            if self._table_exists(catalog_name, schema_name, table_name):
                                #step 5.1: Get the schema of the existing table
                                table_schema = self._get_table_schema(catalog_name, schema_name, table_name)
                                #step 5.2:  Apply the schema to the table
                                df = self._impose_table_schema(df, table_schema)
                            else:
                                print("Table does not exist")

            
                            #step6: Write df to the table
                            previous_row_count_in_table, new_row_count_in_table = self._write_to_table(df, catalog_name, schema_name, table_name,table_mode,partition_column)

                            #step7: Move processed files to archive path
                            self._move_processed_files_to_archieve(path, archival_path, list_of_files)
                            
                            #step8: Log the ingestion audit
                            self._log_ingestion_audit(
                                entity_name=table_name,
                                audit_catalog_name=audit_catalog_name,
                                audit_schema_name=audit_schema_name,
                                audit_table_name=audit_table_name,
                                source_path=path,
                                file_pattern=pattern,
                                file_names=list_of_files,
                                table_name=f"{catalog_name}.{schema_name}.{table_name}",
                                previous_row_count_in_table=previous_row_count_in_table,
                                new_row_count_in_table=new_row_count_in_table,
                                file_row_count=file_count,
                                status="SUCCESS"
                            )
                            
                        else:
                            self.logger.info(f"the dataframe is empty ")
                    else:
                        self.logger.info(f"There are no files in the {path} matching the pattern {pattern}")
                    
                    
                
                except Exception as e:
                    # If any exception occurs, log the error to the audit table
                    error_message = str(e)
                    error_stacktrace = traceback.format_exc()
                    
                    self._log_ingestion_audit(
                        entity_name=table_name,
                        audit_catalog_name=audit_catalog_name,
                        audit_schema_name=audit_schema_name,
                        audit_table_name=audit_table_name,
                        source_path=path,
                        file_pattern=pattern,
                        file_names=list_of_files if 'list_of_files' in locals() else [],
                        table_name=f"{catalog_name}.{schema_name}.{table_name}",
                        previous_row_count_in_table=None,
                        new_row_count_in_table=None,
                        file_row_count=None,
                        status="FAILURE",
                        error_message=error_message
                    )
                    self.logger.info(f"Error occurred for {table_name}: {error_message}")
                    self.logger.info(f"Error details: {error_stacktrace}")
                for h in self.logger.handlers:
                    if hasattr(h, "flush"):
                        h.flush()

# COMMAND ----------


