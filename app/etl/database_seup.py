from app.etl.etl_abstract import EtlProcessor


class DatabaseSetup(EtlProcessor):

    def cleanup(self):
        self.spark.sql(f"DROP DATABASE IF EXISTS {self.config.bronze_db_name} CASCADE")
        self.spark.sql(f"DROP DATABASE IF EXISTS {self.config.silver_db_name} CASCADE")
        self.spark.sql(f"DROP DATABASE IF EXISTS {self.config.gold_db_name} CASCADE")

    def create_bronze_database(self):
        print("Creating dataset")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.bronze_db_name}")
        print(f"Crated database {self.config.bronze_db_name}")

    def create_silver_database(self):
        print("Creating dataset")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.silver_db_name} LOCATION '{self.config.silver_location}'")
        print(f"Crated database {self.config.silver_db_name}")

    def create_gold_database(self):
        print("Creating dataset")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.gold_db_name} LOCATION '{self.config.gold_location}'")
        print(f"Crated database {self.config.gold_db_name}")

    def process(self, date=None):
        self.cleanup()
        self.create_bronze_database()
        self.create_silver_database()
        self.create_gold_database()
