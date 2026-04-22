import os
import wget
import sys
import shutil
import zipfile

sys.stdout.fileno = lambda: False  # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'


class DatabricksHelpers:
    def __init__(self, dbutils, exercise_name, spark_session, volume="raw_data"):
        self.dbutils = dbutils
        self.exercise_name = exercise_name
        self.spark_session = spark_session
        self.volume = volume
        self.initialize_catalog()
        

    def current_catalog(self) -> str:
        return f"{self.current_user().split(".")[0]}s_catalog"

    def current_user(self) -> str:
        return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]

    def volume_directory(self) -> str:
        #return f"/FileStore/{self.current_user()}/{self.exercise_name}"
        return f"/Volumes/{self.current_catalog()}/{self.exercise_name}/{self.volume}"
    
    def schemas_directory(self) -> str:
        #return f"/FileStore/{self.current_user()}/{self.exercise_name}"
        return f"/Volumes/{self.current_catalog()}/{self.exercise_name}/schemas"
    def checkpoints_directory(self) -> str:
        #return f"/FileStore/{self.current_user()}/{self.exercise_name}"
        return f"/Volumes/{self.current_catalog()}/{self.exercise_name}/checkpoints"
    
    
    # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-catalog
    def initialize_catalog(self):
        catalog=self.current_catalog()
        self.spark_session.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        self.spark_session.sql(f"USE CATALOG {catalog}")
        self.spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.exercise_name}")
        self.spark_session.sql(f"USE SCHEMA {self.exercise_name}")
        self.spark_session.sql(f"CREATE VOLUME IF NOT EXISTS {self.volume}")
        self.spark_session.sql(f"CREATE VOLUME IF NOT EXISTS schemas")
        self.spark_session.sql(f"CREATE VOLUME IF NOT EXISTS checkpoints")

    def table_location(self):
        return f"{self.current_catalog()}.{self.exercise_name}"
    

    def tmp_working_directory(self) -> str:
        return f"{os.getcwd()}/tmp"
    
    def folder_exists(self,path):
        try:
            dbutils.fs.ls(path)
            return True
        except Exception:
            return False

    def clean_working_directory(self):
        print(f"Cleaning up/removing files in {self.working_directory()}")
        self.dbutils.fs.rm(self.working_directory(), True)

    def table_exists(self,table_name):
        return self.spark_session.catalog.tableExists(table_name)
    
    # def clean_user_directory(self):
    #     dir = f"/FileStore/{self.current_user()}"
    #     print(f"Cleaning up/removing files in {dir}")
    #     self.dbutils.fs.rm(dir, True)

    def clean_remake_dir(self):
        if os.path.isdir(self.tmp_working_directory()): shutil.rmtree(self.tmp_working_directory())
        os.makedirs(self.tmp_working_directory())


    def stop_all_streams(self, spark):
        print("Stopping all streams")
        for s in spark.streams.active:
            try:
                s.stop()
            except:
                pass
        print("Stopped all streams")
        return