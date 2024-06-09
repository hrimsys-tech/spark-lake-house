from pyspark.sql import Window
from pyspark.sql.functions import count, when, col, desc, rank, sum

from app.etl.etl_abstract import EtlProcessor


class DriversStanding(EtlProcessor):
    def process(self, date=None):
        race_results = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.gold_db_name}.race_results",
        ).filter(col('ingestion_date') == date)

        driver_standings_df = (
            race_results
            .groupBy("race_year", "driver_name", "driver_nationality")
            .agg(
                sum("points").alias("total_points"),
                count(when(col("position") == 1, True)).alias("wins")
            )
        )

        driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
        final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

        ordered_df = final_df.orderBy(desc("race_year"))
        merge_condition = """
            target.driver_name = source.driver_name AND target.race_year = source.race_year
        """
        self.upsert_data(
            df=ordered_df,
            table=f'{self.config.gold_db_name}.drivers_standings',
            file_path=None,
            file_format='delta',
            partition_by=['race_year'],
            merge_condition=merge_condition,
        )
