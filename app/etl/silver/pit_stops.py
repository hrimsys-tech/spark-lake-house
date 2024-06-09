from pyspark.sql.functions import col

from app.etl.etl_abstract import EtlProcessor


class SilverPitStops(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.pit_stops",
        ).filter(col('ingestion_date') == date)

        pit_stops_df = (
            df
            .withColumnRenamed("raceId", "race_id")
            .withColumnRenamed("driverId", "driver_id")
        )

        merge_condition = """
            target.race_id = source.race_id AND 
            target.driver_id = source.driver_id AND
            target.stop = source.stop AND 
            target.race_id = source.race_id
        """

        self.upsert_data(
            df=pit_stops_df,
            table=f'{self.config.silver_db_name}.pit_stops',
            file_path=None,
            file_format='delta',
            partition_by=['race_id'],
            merge_condition=merge_condition
        )
