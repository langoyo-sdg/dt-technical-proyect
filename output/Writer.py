import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

class Writer:
    """
    A class for writing DataFrames to various sinks based on metadata configuration.

    Attributes:
        spark (SparkSession): The Spark session to use for writing data.
        logger (logging.Logger): Logger instance to log information and errors.

    Methods:
        __init__(spark_session):
            Initializes the Writer with a Spark session and logger.
        write_dataframes(sources: dict, sinks: dict):
            Writes DataFrames to sinks based on metadata configuration.
        write_to_sink(df: DataFrame, format: str, save_mode: str, paths: list):
            Writes a DataFrame to specified paths in the given format and save mode.
    """
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(self.__class__.__name__)

    def write_dataframes(self, sources: dict, sinks: dict):
        """
        Write DataFrames to sinks based on metadata configuration.

        Args:
            sources (dict): A dictionary where the keys are source names and the values are DataFrame objects to be written.
            sinks (dict): A list of dictionaries, each representing a sink configuration. Each sink dictionary must contain:
                - "input": The name of the source DataFrame to process.
                - "name": The type of sink, typically "ok" (for valid data) or "ko" (for invalid data).
                - "paths": A list of paths where the DataFrame will be saved.
                - "format": The file format for saving (e.g., "JSON", "CSV").
                - "saveMode": The save mode (e.g., "OVERWRITE", "APPEND").

        Returns:
            None: The function writes DataFrames to specified sinks without returning any value.

        Raises:
            RuntimeError: If an error occurs during the processing of a sink or writing the DataFrame, a RuntimeError is raised.
        """

        for sink in sinks:
            try:
                # Getting the source specified in the sink
                input_name = sink["input"]
                df = sources[input_name]

                # Extracting sink parameters
                sink_name = sink.get("name", "unkonwn")
                paths = sink['paths']
                sink_type = sink['type']
                format = sink.get("format", "JSON")
                save_mode = sink.get("saveMode", "OVERWRITE")

                # Check if the DataFrame contains validation errors and split dataframe accordingly
                if "validation_errors" in df.columns:
                    valid_df = df.filter(col("validation_errors") == "[]")
                    valid_df = valid_df.drop(col("validation_errors"))
                    invalid_df = df.filter(col("validation_errors") != "[]")
                else:
                    valid_df = df
                    invalid_df = self.spark.createDataFrame([], schema=df.schema)

                # Log the sink details
                self.logger.info(f"Processing sink '{sink_name}' for input '{input_name}'.")

                # There are three types of sink. Each determines the which split of the dataset to use. Forcing to use one of these. 
                if sink_type == "ok":
                    self.write_to_sink(df=valid_df, format=format, save_mode=save_mode, paths=paths)
                elif sink_type == "ko":
                    self.write_to_sink(df=invalid_df, format=format, save_mode=save_mode, paths=paths)
                elif sink_type == "all":
                    self.write_to_sink(df=df, format=format, save_mode=save_mode, paths=paths)
                else:
                    self.logger.error(f"Error processing sink '{sink.get('name', 'unknown')}' for input '{sink.get('input', 'unknown')}': Sink type must be 'ok', 'ko' or 'all")
                    raise ValueError(f"Error processing sink '{sink.get('name', 'unknown')}' for input '{sink.get('input', 'unknown')}': Sink type must be 'ok', 'ko' or 'all")
                
                self.logger.info(f"Successfully processed sink '{sink_name}' for input '{input_name}'.")

            except Exception as e:
                self.logger.error(f"Error processing sink '{sink.get('name', 'unknown')}' for input '{sink.get('input', 'unknown')}': {e}")
                raise RuntimeError(f"Error processing sink '{sink.get('name', 'unknown')}' for input '{sink.get('input', 'unknown')}': {e}")


    def write_to_sink(self, df: DataFrame, format: str, save_mode: str, paths):
        """
        Write a DataFrame to the specified paths in the given format and save mode.

        Args:
            df (DataFrame): The DataFrame to be written.
            format (str): The file format in which the DataFrame will be saved (e.g., "JSON", "CSV").
            save_mode (str): The save mode for writing the DataFrame (e.g., "OVERWRITE", "APPEND").
            paths (list of str): List of output paths where the DataFrame will be saved.

        Returns:
            None: The function writes the DataFrame to the specified paths without returning any value.

        Raises:
            RuntimeError: If an error occurs during the writing process, a RuntimeError is raised.
        """
        print(df, format, paths)
        for path in paths:
            try:
                self.logger.info(f"Writing DataFrame to path: {path} with format: {format} and save mode: {save_mode}")
                writer = df.write.format(format).mode(save_mode)
                # Add header option for CSV format
                if format.lower() == "csv":
                    writer = writer.option("header", "true")
                writer.save(path)
                self.logger.info(f"Successfully wrote DataFrame to path: {path}")
            except Exception as e:
                self.logger.error(f"Error writing DataFrame to path '{path}': {e}")
                raise RuntimeError(f"Error writing DataFrame to path '{path}': {e}")
