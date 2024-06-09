from pyspark.sql.functions import current_timestamp, lit

from app.etl.etl_abstract import EtlProcessor


class RawPitStops(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_path=f"{self.config.landing_zone}/{date}/pit_stops.json",
            file_format='json',
            schema=self.generate_schema(RawPitStops.__name__),
            multiLine='true'
        )

        pit_stops_df = (
            df.withColumn('ingestion_timestamp', current_timestamp())
            .withColumn('ingestion_date', lit(date))
        )

        self.write_data(
            df=pit_stops_df,
            mode='append',
            file_format='delta',
            partition_by=None,
            table=f'{self.config.bronze_db_name}.pit_stops',
            path=f'{self.config.bronze_location}/pit_stops',
        )

