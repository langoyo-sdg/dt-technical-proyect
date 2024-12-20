import os
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType, BooleanType, BinaryType, ShortType, ByteType, DoubleType, DateType, LongType
from pyspark.sql import functions as F
import logging

class Reader:
    """
    A class for reading data sources and loading them into Spark DataFrames

    Attributes:
        spark (SparkSession): The Spark session to use for reading data.
        logger (logging.Logger): Logger instance to log information and errors.

    Methods:
        __init__(spark):
            Initializes the Reader with a Spark session and logger.
        schema_conversion(schema_list: list) -> StructType:
            Converts ths schema defined in the metadata into a Spark StructType schema for DataFrame creation.
        load_df(metadata_sources: list) -> dict:
            Reads all data sources defined in the metadata and loads them into DataFrames.
    """
    def __init__(self, spark):
        self.spark = spark
        # Logger for this class
        self.logger = logging.getLogger(self.__class__.__name__)  


    def schema_conversion(self, schema_list: list) -> StructType:
        """
        Converts the metadata schema nito a Spark StructType schema for DataFrame creation.

        Args:
            schema_list (list of dict): A list of dictionaries where each dictionary represents a field with:
                - "field": The name of the field (string).
                - "type": The type of the field (string), which should match one of the supported Spark data types.

        Returns:
            StructType: A Spark StructType object representing the schema, created from the provided field names and types.

        Raises:
            RuntimeError: If an error occurs during schema conversion, a RuntimeError is raised.
        """
        spark_types = {
            "STRING": "StringType()",
            "TIMESTAMP": "TimestampType()",
            "FLOAT": "FloatType()",
            "INT": "IntegerType()",
            "INTEGER": "IntegerType()",
            "BOOLEAN": "BooleanType()",
            "BINARY": "BinaryType()",
            "SHORT": "ShortType()",
            "BYTE": "ByteType()",
            "DOUBLE": "DoubleType()",
            "DATE": "DateType()",
            "LONG": "LongType()"
        }
    
        struct = ""
        try:
            for field in schema_list:
        
                field_name = field['field']
                field_type = field['type']
                type_list = spark_types.keys()
        
                if field_type.upper() in type_list:
                    struct_type = spark_types[field_type]
                    struct += f"StructField('{field_name}', {struct_type}, nullable={True}),"
        
            struct_format = f"StructType([{struct[:-1]}])"

            self.logger.info("Schema conversion successful: " + str(struct_format))
            return eval(struct_format)
        
        except Exception as e:
            self.logger.error(f"Error in schema conversion: {e}")
            raise RuntimeError(f"Error in schema conversion: {e}")


    def load_df(self, metadata_sources: list) -> dict:
        """
        Reads all data sources defined in the metadata and loads them into DataFrames.

        Args:
            metadata_sources (list of dict): A list of dictionaries where each dictionary represents a data source with:
                - "name": The name of the source (string).
                - "format": The format to read the data (e.g., "JSON", "CSV").
                - "schema": The schema for the data (optional; list of field names and types).
                - "options": A dictionary of options for reading the data (optional).
                - "paths": A list of paths to the data files.

        Returns:
            dict: A dictionary where keys are the source names and values are the corresponding DataFrames loaded from the sources.

        Raises:
            RuntimeError: If an error occurs during the reading process of any source, a RuntimeError is raised.
        """
        sources = {}
        try:
            for source in metadata_sources:
                # Retrieve the schema from metadata
                schema = source.get("schema", [])
                schema = self.schema_conversion(schema)
                # Retrieve reading options from metadata
                options = source.get("options", {})
                self.logger.info("Options rertrieved: " + str(options))
                # Load the data into a df
                if schema:
                    df = self.spark.read.format(source["format"]).options(**options).schema(schema).load(source["paths"])
                else:
                    df = self.spark.read.format(source["format"]).options(**options).load(source["paths"])

                sources[source["name"]] = df

            self.logger.info("Data sources loaded successfully.")
            return sources
        
        except Exception as e:
            self.logger.info(f"Error reading sources: {e}")
            raise RuntimeError(f"Error reading sources: {e}")
