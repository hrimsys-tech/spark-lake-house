from pyspark.sql.functions import current_timestamp, lit

from app.etl.etl_abstract import EtlProcessor


class RawQualifying(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/qualifying/*.json",
            file_format='json',
            schema=self.generate_schema(RawQualifying.__name__),
            tabel=None,
            multiLine='true'
        )

        qualifying_df = (
            df.withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
        )

        self.write_data(
            df=qualifying_df,
            mode='append',
            file_format='delta',
            table=f'{self.config.bronze_db_name}.qualifying',
            partition_by=None,
            path=f'{self.config.bronze_location}/qualifying',
        )
