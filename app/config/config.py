class Config:
    def __init__(self):
        self.max_file_per_trigger = 1000
        self.catalog_name = 'spark_catalog'
        self.bronze_db_name = f"{self.catalog_name}.bronze"
        self.silver_db_name = f"{self.catalog_name}.silver"
        self.gold_db_name = f"{self.catalog_name}.gold"
        self.landing_zone = "s3a://formula1/landing_zone"
        self.bronze_location = "s3a://formula1/warehouse/bronze"
        self.silver_location = "s3a://formula1/warehouse/silver"
        self.gold_location = "s3a://formula1/warehouse/gold"
