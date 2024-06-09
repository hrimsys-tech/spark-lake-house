from app.etl.etl_abstract import EtlProcessor


class SilverConstructors(EtlProcessor):
    def process(self, date=None):
        df = self.read_data(
            file_format='delta',
            file_path=None,
            tabel=f"{self.config.bronze_db_name}.constructors",
        )

        constructors_df = (
            df
            .withColumnRenamed('constructorId', 'constructor_id')
            .withColumnRenamed('constructorRef', 'constructor_ref')
        )

        self.write_data(
            df=constructors_df,
            mode='overwrite',
            file_path=None,
            file_format='delta',
            table=f'{self.config.silver_db_name}.constructors',
            partition_by=None
        )
