from pyspark.sql.functions import current_timestamp, col
from app.etl.etl_abstract import EtlProcessor


class SilverResults(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.results",
        ).filter(col('ingestion_date') == date)

        results_df = (
            df
            .withColumnRenamed("resultId", "result_id")
            .withColumnRenamed("raceId", "race_id")
            .withColumnRenamed("driverId", "driver_id")
            .withColumnRenamed("constructorId", "constructor_id")
            .withColumnRenamed("positionText", "position_text")
            .withColumnRenamed("positionOrder", "position_order")
            .withColumnRenamed("FastestLap", "fastest_lap")
            .withColumnRenamed("FastestLapTime", "fastest_lap_time")
            .withColumnRenamed("FastestLapSpeed", "fastest_lap_speed")
            .drop_duplicates(['result_id', 'driver_id'])
        )

        merge_condition = """
            target.result_id = source.result_id AND
            target.race_id = source.race_id
        """

        self.upsert_data(
            df=results_df,
            table=f'{self.config.silver_db_name}.results',
            file_path=None,
            file_format='delta',
            partition_by=['race_id'],
            merge_condition=merge_condition,
        )
