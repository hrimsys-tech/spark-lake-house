from pyspark.sql.functions import current_timestamp, col, lit

from app.etl.etl_abstract import EtlProcessor


class RaceResults(EtlProcessor):
    def process(self, date=None):
        drivers_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.drivers",
        )

        constructor_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.constructors",
        )

        circuit_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.circuits",
        )

        races_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.races",
        )

        results_df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.silver_db_name}.results",
        ).filter(col('ingestion_date') == date)

        race_circuit_df = (
            races_df
            .join(circuit_df, how='inner', on=races_df.circuit_id == circuit_df.circuit_id)
            .select(
                races_df.race_id,
                races_df.race_year,
                races_df.name.alias('race_name'),
                races_df.race_timestamp.alias('race_date'),
                circuit_df.location
            )
        )

        race_results_df = (
            results_df
            .join(race_circuit_df, how='inner', on=results_df.race_id == race_circuit_df.race_id)
            .join(drivers_df, how='inner', on=results_df.driver_id == drivers_df.driver_id)
            .join(constructor_df, how='inner', on=results_df.constructor_id == constructor_df.constructor_id)
        ).select(
            race_circuit_df.race_id,
            race_circuit_df.race_year,
            race_circuit_df.race_date,
            race_circuit_df.location,
            drivers_df.name.alias('driver_name'),
            drivers_df.number.alias('driver_number'),
            drivers_df.nationality.alias('driver_nationality'),
            results_df.grid,
            results_df.position,
            results_df.fastest_lap,
            results_df.points,
            constructor_df.name.alias('team'),
            results_df.time.alias('race_time')
        ).withColumn('ingestion_timestamp', current_timestamp()).withColumn('ingestion_date', lit(date))

        merge_condition = """
            target.driver_name = source.driver_name AND target.race_id = source.race_id
        """
        self.upsert_data(
            df=race_results_df,
            table=f'{self.config.gold_db_name}.race_results',
            file_path=None,
            file_format='delta',
            partition_by=['race_id'],
            merge_condition=merge_condition,
            mergeSchema="true"
        )
