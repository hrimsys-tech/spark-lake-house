from pyspark.sql.functions import to_timestamp, concat, lit, col

from app.etl.etl_abstract import EtlProcessor


class SilverRaces(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.races",
        ).filter(col('ingestion_date') == date)

        races_df = (
            df
            .withColumnRenamed('circuitId', 'circuit_id')
            .withColumnRenamed('raceId', 'race_id')
            .withColumnRenamed('year', 'race_year')
            .withColumn('race_timestamp',
                        to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
        )

        self.write_data(
            df=races_df,
            mode='overwrite',
            file_path=None,
            file_format='delta',
            table=f'{self.config.silver_db_name}.races',
            partition_by=None
        )
