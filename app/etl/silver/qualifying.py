from pyspark.sql.functions import col
from app.etl.etl_abstract import EtlProcessor


class SilverQualifying(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.qualifying",
        ).filter(col('ingestion_date') == date)

        qualifying_df = (
            df
            .withColumnRenamed("qualifyId", "qualify_id")
            .withColumnRenamed("raceId", "race_id")
            .withColumnRenamed("driverId", "driver_id")
            .withColumnRenamed("constructorId", "constructor_id")
        )

        merge_condition = """
            target.qualify_id = source.qualify_id AND
            target.race_id = source.race_id
        """

        self.upsert_data(
            df=qualifying_df,
            table=f'{self.config.silver_db_name}.qualifying',
            file_path=None,
            file_format='delta',
            partition_by=['race_id'],
            merge_condition=merge_condition,
        )
