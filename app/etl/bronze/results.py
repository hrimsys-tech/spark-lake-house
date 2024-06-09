from pyspark.sql.functions import current_timestamp, lit
from app.etl.etl_abstract import EtlProcessor


class RawResults(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/results.json",
            file_format='json',
            schema=self.generate_schema(RawResults.__name__),
        )

        results_df = (
            df.withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
            .drop('statusId')
        )

        self.write_data(
            df=results_df,
            mode='append',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.results',
            partition_by=None,
            path=f'{self.config.bronze_location}/results',
        )
