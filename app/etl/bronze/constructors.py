from pyspark.sql.functions import current_timestamp, lit
from app.etl.etl_abstract import EtlProcessor


class RawConstructors(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/constructors.json",
            file_format='json',
            schema=self.generate_schema(RawConstructors.__name__)
        )

        constructors_df = (
            df
            .withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
            .drop('url')
        )

        self.write_data(
            df=constructors_df,
            mode='overwrite',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.constructors',
            partition_by=None,
            path=f'{self.config.bronze_location}/constructors'
        )
