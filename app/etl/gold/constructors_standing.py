from pyspark.sql import Window
from pyspark.sql.functions import count, when, col, desc, rank, sum

from app.etl.etl_abstract import EtlProcessor


class ConstructorsStanding(EtlProcessor):
    def process(self, date=None):
        race_results_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.gold_db_name}.race_results",
        ).filter(col('ingestion_date') == date).select(
            "race_year",
            "team",
            "position",
            "points"
        )
        constructor_standings_df = (
            race_results_df
            .groupBy("race_year", "team")
            .agg(
                sum("points").alias("total_points"),
                count(when(col("position") == 1, True)).alias("wins"))
        )

        constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

        final_df = (
            constructor_standings_df
            .withColumn("rank", rank().over(constructor_rank_spec))
            .select(
                "rank",
                "race_year",
                "team",
                "wins",
                "total_points"
            ).withColumnRenamed("total_points", "points")
        )

        ordered_df = final_df.orderBy(desc("race_year"))

        merge_condition = "target.team = source.team AND target.race_year = source.race_year"

        self.upsert_data(
            df=ordered_df,
            table=f'{self.config.gold_db_name}.constructor_standings',
            file_path=None,
            file_format='delta',
            partition_by=['race_year'],
            merge_condition=merge_condition,
        )
