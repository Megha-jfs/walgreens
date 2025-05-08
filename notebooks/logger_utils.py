# Databricks notebook source


# COMMAND ----------

# dbutils.fs.head("dbfs:/user/jyothsna.tadi@databricks.com/logs/Rdi1_20250503_154920.log")  #Reads first 1000 bytes


# COMMAND ----------

# dbutils.fs.ls("/Volumes/jyothsna_tadi/rdi/logs/")

# COMMAND ----------

import logging
import io
import datetime

class DBFSFileHandler(logging.Handler):
    def __init__(self, dbfs_path: str):
        super().__init__()
        self.dbfs_path = dbfs_path
        self.log_buffer = io.StringIO()

    def emit(self, record):
        log_entry = self.format(record)
        self.log_buffer.write(log_entry + "\n")
    
    def flush(self):
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        dbutils.fs.put(self.dbfs_path, self.log_buffer.getvalue(), overwrite=True)
        self.log_buffer = io.StringIO()

def get_logger(name: str, dbfs_log_path: str = None, log_file_prefix: str = "app_log") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not getattr(logger, "_dbfs_logger_configured", False):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Console handler (always added)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Add DBFS handler only if path is provided
        if dbfs_log_path:
            dbfs_log_file = f"{dbfs_log_path.rstrip('/')}/{log_file_prefix}_{timestamp}.log"
            fh = DBFSFileHandler(dbfs_log_file)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        logger._dbfs_logger_configured = True

    return logger


# COMMAND ----------

# DBTITLE 1,working
# import logging
# import io
# import datetime

# class DBFSFileHandler(logging.Handler):
#     def __init__(self, dbfs_path: str):
#         super().__init__()
#         self.dbfs_path = dbfs_path
#         self.log_buffer = io.StringIO()

#     def emit(self, record):
#         log_entry = self.format(record)
#         self.log_buffer.write(log_entry + "\n")
    

#     def flush(self):
#         from pyspark.dbutils import DBUtils
#         dbutils = DBUtils(spark)
#         dbutils.fs.put(self.dbfs_path, self.log_buffer.getvalue(), overwrite=True)
#         self.log_buffer = io.StringIO()

# def get_logger(name: str, dbfs_log_path: str = "dbfs:/tmp/logs/", log_file_prefix: str = "app_log") -> logging.Logger:
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.DEBUG)

#     if not getattr(logger, "_dbfs_logger_configured", False):
#         # Construct timestamped log file path
#         timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#         dbfs_log_file = f"{dbfs_log_path.rstrip('/')}/{log_file_prefix}_{timestamp}.log"

#         # Formatter
#         formatter = logging.Formatter(
#             fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#             datefmt="%Y-%m-%d %H:%M:%S"
#         )

#         # Console handler
#         ch = logging.StreamHandler()
#         ch.setFormatter(formatter)
#         logger.addHandler(ch)

#         # DBFS file handler
#         fh = DBFSFileHandler(dbfs_log_file)
#         fh.setFormatter(formatter)
#         logger.addHandler(fh)

#         logger._dbfs_logger_configured = True

#     return logger


# COMMAND ----------


