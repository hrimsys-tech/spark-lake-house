from pyspark.sql.functions import current_timestamp, lit
from app.etl.etl_abstract import EtlProcessor


class RawDrivers(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/drivers.json",
            file_format='json',
            schema=self.generate_schema(RawDrivers.__name__),
        )
        drivers_df = (
            df
            .withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
            .drop('url')
        )

        self.write_data(
            df=drivers_df,
            mode='overwrite',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.drivers',
            partition_by=None,
            path=f'{self.config.bronze_location}/drivers',
        )
