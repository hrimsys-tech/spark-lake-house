from pyspark.sql.functions import col

from app.etl.etl_abstract import EtlProcessor


class SilverLapTimes(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.lap_times",
        ).filter(col('ingestion_date') == date)

        lap_times_df = (
            df
            .withColumnRenamed("raceId", "race_id")
            .withColumnRenamed("driverId", "driver_id")
        )

        merge_condition = """
            target.race_id = source.race_id AND 
            target.driver_id = source.driver_id AND
            target.lap = source.lap AND 
            target.race_id = source.race_id
        """

        self.upsert_data(
            df=lap_times_df,
            table=f'{self.config.silver_db_name}.lap_times',
            file_path=None,
            file_format='delta',
            partition_by=['race_id'],
            merge_condition=merge_condition
        )
