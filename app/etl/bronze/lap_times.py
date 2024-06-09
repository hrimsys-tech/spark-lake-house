from pyspark.sql.functions import current_timestamp, lit

from app.etl.etl_abstract import EtlProcessor


class RawLapTimes(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/lap_times",
            file_format='csv',
            schema=self.generate_schema(RawLapTimes.__name__),
            header='true'
        )

        lap_times_df = (
            df.withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
        )

        self.write_data(
            df=lap_times_df,
            mode='append',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.lap_times',
            partition_by=None,
            path=f'{self.config.bronze_location}/lap_times',
            mergeSchema=True
        )
