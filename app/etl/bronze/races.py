from pyspark.sql.functions import current_timestamp, lit

from app.etl.etl_abstract import EtlProcessor


class RawRaces(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/races.csv",
            file_format='csv',
            schema=self.generate_schema(RawRaces.__name__),
            header='true'
        )

        races_df = (
            df
            .withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
            .drop('url')
        )

        self.write_data(
            df=races_df,
            mode='overwrite',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.races',
            partition_by=None,
            path=f'{self.config.bronze_location}/races',
        )
