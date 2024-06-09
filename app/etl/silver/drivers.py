from pyspark.sql.functions import col, concat, lit
from app.etl.etl_abstract import EtlProcessor


class SilverDrivers(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.drivers",
        )

        drivers_df = (
            df
            .withColumnRenamed('driverId', 'driver_id')
            .withColumnRenamed('driverRef', 'driver_ref')
            .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))
        )

        self.write_data(
            df=drivers_df,
            mode='overwrite',
            file_path=None,
            file_format='delta',
            table=f'{self.config.silver_db_name}.drivers',
            partition_by=None
        )
