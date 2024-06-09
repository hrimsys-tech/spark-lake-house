from pyspark.sql.functions import current_timestamp, lit
from app.etl.etl_abstract import EtlProcessor


class RawCircuits(EtlProcessor):

    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/circuits.csv",
            file_format='csv',
            schema=self.generate_schema(RawCircuits.__name__),
            header='true'
        )

        circuits_df = (
            df
            .withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
            .drop('url')
        )

        self.write_data(
            df=circuits_df,
            mode='overwrite',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.circuits',
            partition_by=None,
            path=f'{self.config.bronze_location}/circuits'
        )
