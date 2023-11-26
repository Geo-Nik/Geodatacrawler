"""
Module for handling database data writing operations.

Defines an abstract base class `DBDataWriter` for writing data to a database
and a concrete class `GeoJsonDBDataWriter` that specializes in writing GeoJSON
feature collection data to a PostGIS database table.
"""

from typing import Dict, Any, Optional
import logging
from abc import ABC, abstractmethod
import geopandas as gpd
from sqlalchemy import create_engine, exc


class DBDataWriter(ABC):
    """
    Abstract base class for writing data to a database.
    """

    def __init__(self, db_connection: Dict[str, Any]) -> None:
        """
        Initializes a DBDataWriter instance.

        Args:
            db_connection (Dict[str, Any]): Database connection parameters.
        """
        self.db_connection: Dict[str, Any] = db_connection

    @property
    def engine(self) -> Any:
        """
        Property representing the database engine.

        Returns:
            sqlalchemy.engine.base.Engine: Database engine.
        """
        engine = create_engine(
            f"postgresql://{self.db_connection['user']}:{self.db_connection['password']}@"
            f"{self.db_connection['host']}:{self.db_connection['port']}/{self.db_connection['database']}"
        )
        return engine

    @abstractmethod
    def write(self) -> None:
        """
        Abstract method to be implemented by subclasses for writing data to a database.
        """

class GeoJsonDBDataWriter(DBDataWriter):
    """
    Writes GeoJSON feature collection data to a PostGIS database table.
    """

    def __init__(
        self,
        db_connection: Dict[str, Any],
        geojson_feature_collection: Dict[str, Any],
        table_name: str,
        crs: Optional[str] = None,
    ) -> None:
        """
        Initializes a GeoJsonDBDataWriter instance.

        Args:
            db_connection (Dict[str, Any]): Database connection parameters.
            geojson_feature_collection (Dict[str, Any]): GeoJSON feature collection data.
            table_name (str): Name of the database table.
            crs (Optional[str]): Coordinate reference system identifier. Defaults to None.
        """
        super().__init__(db_connection)
        self.geojson_feature_collection: Dict[str, Any] = geojson_feature_collection
        self.table_name: str = table_name
        self.crs: Optional[str] = crs

    def write(self) -> None:
        """
        Writes GeoJSON feature collection data to a PostGIS database table.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: If an error occurs during the data writing process.
        """
        try:
            gdf = gpd.GeoDataFrame.from_features(self.geojson_feature_collection)

            # Set the CRS of the GeoDataFrame
            if self.crs is not None:
                gdf.crs = self.crs

            gdf.to_postgis(
                name=self.table_name, con=self.engine, if_exists="append", index=False
            )

            # Close the database connection
            self.engine.dispose()
            logging.info("GeoJSON data has been successfully written to the PostGIS table.")
        except exc.SQLAlchemyError as e:
            logging.error(f"Error writing GeoJSON data to the database: {e}")
