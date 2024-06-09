import json
import os
from abc import abstractmethod, ABC
from typing import Optional, List

from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, BooleanType,
    LongType, DoubleType, DateType, TimestampType
)

from app.config.config import Config


class EtlProcessor(ABC):
    schema_config_file = os.path.join(os.path.dirname(__file__), 'schemas.json')

    def __init__(self, spark):
        self.spark = spark
        self.config = Config()

    @abstractmethod
    def process(self, date: str = None):
        pass

    def read_data(self, file_path: str = None, file_format: str = 'csv', schema: StructType = None,
                  tabel: str = None, **options) -> DataFrame:
        reader = self.spark.read.format(file_format)
        if schema:
            reader = reader.schema(schema)
        for option, value in options.items():
            reader = reader.option(option, value)

        return reader.table(tabel) if tabel else reader.load(file_path)

    @staticmethod
    def write_data(df: DataFrame, file_format: str = 'parquet', mode: str = 'overwrite', file_path: str = None,
                   table: str = None, partition_by: Optional[List[str]] = None, **options):
        writer = df.write.format(file_format).mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        for option, value in options.items():
            writer = writer.option(option, value)
        print('Table {} written to {}'.format(table, file_path))

        if table:
            writer.saveAsTable(table)
        else:
            writer.save(file_path)

    def upsert_data(self, df: DataFrame, file_format: str = 'parquet', mode: str = 'overwrite', file_path: str = None,
                    table: str = None, merge_condition: str = None, partition_by: Optional[List[str]] = None,
                    **options):
        print('Called upsert table {} ...'.format(table))
        if self.spark._jsparkSession.catalog().tableExists(table):
            table_name_without_catalog = table.replace(f'{self.config.catalog_name}.', "")
            print('Updating existing table {} ...'.format(table_name_without_catalog))
            delta_table = DeltaTable.forName(self.spark, table_name_without_catalog)
            (delta_table.alias('target').merge(
                df.alias('source'),
                merge_condition
            )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        else:
            print('Inserting into table {} ...'.format(table))
            self.write_data(df, file_format, mode, file_path, table, partition_by)

    @staticmethod
    def generate_schema(class_name: str) -> StructType:
        def parse_field(field):
            name = field['name']
            dtype = field['type']
            nullable = field.get('nullable', True)
            field_type = None
            if dtype == 'string':
                field_type = StringType()
            elif dtype == 'integer':
                field_type = IntegerType()
            elif dtype == 'float':
                field_type = FloatType()
            elif dtype == 'boolean':
                field_type = BooleanType()
            elif dtype == 'long':
                field_type = LongType()
            elif dtype == 'double':
                field_type = DoubleType()
            elif dtype == 'date':
                field_type = DateType()
            elif dtype == 'timestamp':
                field_type = TimestampType()
            elif dtype == 'struct':
                field_type = StructType([parse_field(f) for f in field['fields']])
            return StructField(name, field_type, nullable)

        # Load the schema configuration from the JSON file
        with open(EtlProcessor.schema_config_file, 'r') as file:
            schema_config = json.load(file).get(class_name, {}).get('fields', [])

        # Generate the schema based on the configuration
        fields = [parse_field(field) for field in schema_config]
        return StructType(fields)
