from app.etl.etl_abstract import EtlProcessor


class SilverCircuits(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.circuits",
        )

        circuits_df = (
            df
            .withColumnRenamed('circuitId', 'circuit_id')
            .withColumnRenamed('circuitRef', 'circuit_ref')
            .withColumnRenamed('lat', 'latitude')
            .withColumnRenamed('lng', 'longitude')
            .withColumnRenamed('country', 'race_country')
        )

        self.write_data(
            df=circuits_df,
            mode='overwrite',
            file_path=None,
            file_format='delta',
            table=f'{self.config.silver_db_name}.circuits',
            partition_by=None
        )
