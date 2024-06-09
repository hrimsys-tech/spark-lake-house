from app.etl.database_seup import DatabaseSetup

from app.etl.bronze.circuits import RawCircuits
from app.etl.bronze.constructors import RawConstructors
from app.etl.bronze.drivers import RawDrivers
from app.etl.bronze.lap_times import RawLapTimes
from app.etl.bronze.pit_stops import RawPitStops
from app.etl.bronze.qualifying import RawQualifying
from app.etl.bronze.races import RawRaces
from app.etl.bronze.results import RawResults

from app.etl.silver.circuits import SilverCircuits
from app.etl.silver.constructors import SilverConstructors
from app.etl.silver.drivers import SilverDrivers
from app.etl.silver.lap_times import SilverLapTimes
from app.etl.silver.pit_stops import SilverPitStops
from app.etl.silver.qualifying import SilverQualifying
from app.etl.silver.races import SilverRaces
from app.etl.silver.results import SilverResults

from app.etl.gold.constructors_standing import ConstructorsStanding
from app.etl.gold.drivers_standing import DriversStanding
from app.etl.gold.race_results import RaceResults
from app.etl.gold.calculate_race_results import CalculateRaceResults


class Run:
    def __init__(self, spark, date):
        self.spark = spark
        self.ingestion_date = date

    def _create_database(self):
        database = DatabaseSetup(self.spark)
        database.process()

    def _bronze(self):
        bronze_classes = [
            RawCircuits,
            RawConstructors,
            RawDrivers,
            RawLapTimes,
            RawPitStops,
            RawQualifying,
            RawResults,
            RawRaces
        ]

        for bronze_class in bronze_classes:
            instance = bronze_class(spark=self.spark)
            instance.process(self.ingestion_date)

    def _silver(self):
        silver_classes = [
            SilverCircuits,
            SilverConstructors,
            SilverDrivers,
            SilverLapTimes,
            SilverPitStops,
            SilverQualifying,
            SilverResults,
            SilverRaces
        ]

        for silver_class in silver_classes:
            instance = silver_class(spark=self.spark)
            instance.process(self.ingestion_date)

    def _presentation(self):
        presentations = [
            RaceResults,
            DriversStanding,
            ConstructorsStanding,
            CalculateRaceResults
        ]

        for presentation in presentations:
            instance = presentation(spark=self.spark)
            instance.process(self.ingestion_date)

    def initialize(self):
        # self._create_database()
        self._bronze()
        self._silver()
        self._presentation()
