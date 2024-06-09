from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

from app.etl.etl_abstract import EtlProcessor


class CalculateRaceResults(EtlProcessor):
    def process(self, date=None):
        results_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.results",
        ).filter(col('ingestion_date') == date)

        drivers_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.drivers",
        )

        constructors_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.constructors",
        )

        races_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.races",
        )

        race_results_updated_df = (
            (
                results_df
                .join(drivers_df, results_df.driver_id == drivers_df.driver_id)
                .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
                .join(races_df, results_df.race_id == races_df.race_id)
                .filter((results_df.position <= 10))
                .select(
                    races_df.race_year,
                    constructors_df.name.alias("team_name"),
                    drivers_df.driver_id,
                    drivers_df.name.alias("driver_name"),
                    races_df.race_id,
                    results_df.position,
                    results_df.points,
                    (11 - results_df.position).alias("calculated_points")
                )
            )
            .withColumn("updated_date", current_timestamp())
            .withColumn("created_date", current_timestamp()))

        if self.spark._jsparkSession.catalog().tableExists(f"{self.config.gold_db_name}.calculated_race_results"):
            table_name_without_catalog = (f"{self.config.gold_db_name}.calculated_race_results"
                                          .replace(f'{self.config.catalog_name}.', ""))

            delta_table = DeltaTable.forName(self.spark, table_name_without_catalog)

            delta_table.alias("tgt").merge(
                race_results_updated_df.alias("upd"),
                "tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id"
            ).whenMatchedUpdate(set={
                "position": col("upd.position"),
                "points": col("upd.points"),
                "calculated_points": col("upd.calculated_points"),
                "updated_date": current_timestamp()
            }).whenNotMatchedInsert(values={
                "race_year": col("upd.race_year"),
                "team_name": col("upd.team_name"),
                "driver_id": col("upd.driver_id"),
                "driver_name": col("upd.driver_name"),
                "race_id": col("upd.race_id"),
                "position": col("upd.position"),
                "points": col("upd.points"),
                "calculated_points": col("upd.calculated_points"),
                "created_date": current_timestamp()
            }).execute()
        else:
            self.write_data(
                df=race_results_updated_df,
                mode='overwrite',
                file_path=None,
                file_format='delta',
                table=f'{self.config.gold_db_name}.calculated_race_results',
                partition_by=None
            )
