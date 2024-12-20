import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from transformations.TranformationBase import TransformationBase

class AddFields(TransformationBase):
    """
    A class for adding new fields to a DataFrame based on specified functions in metadata.

    Methods:
        apply(df: DataFrame, params: dict) -> DataFrame:
            Adds new fields to the DataFrame according to the specified transformation functions.
        current_timestamp(df: DataFrame, col_name: str) -> DataFrame:
            Adds a column with the current timestamp to the DataFrame.

    """
    def apply(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Adds new fields to the DataFrame based on the specified functions.

        Args:
            df (DataFrame): The DataFrame to which new fields will be added.
            params (dict): A dictionary containing the transformation parameters with:
                - "addFields": A list of fields to add, where each field is defined by:
                    - "name": The name of the field to add.
                    - "function": The function to apply (e.g., "current_timestamp").

        Returns:
            DataFrame: The DataFrame with the newly added fields.

        Raises:
            ValueError: If an unknown function is specified for adding fields.
            RuntimeError: If an error occurs while adding fields.

        """
        try:
            add_fields = params.get("addFields", [])
            for field in add_fields:
                name = field["name"]
                function = field["function"]
                if function == "current_timestamp":
                    df = self.current_timestamp(df, name)
                    self.logger.info(f"Added field '{name}' with function '{function}'.")
                else:
                    error_message = f"Unknown function: {function}"
                    self.logger.error(error_message)
                    raise ValueError(error_message)
            return df
        except Exception as e:
            self.logger.error(f"Error in AddFields: {e}")
            raise

    def current_timestamp(self, df: DataFrame, col_name: str) -> DataFrame:
        """
        Adds a column with the current timestamp to the DataFrame.

        Args:
            df (DataFrame): The DataFrame to which the timestamp column will be added.
            col_name (str): The name of the new timestamp column.

        Returns:
            DataFrame: The DataFrame with the new timestamp column added.

        Raises:
            RuntimeError: If an error occurs while adding the timestamp column.

        """
        try:
            self.logger.info(f"Adding current timestamp column '{col_name}'.")
            return df.withColumn(col_name, current_timestamp())
        except Exception as e:
            self.logger.error(f"Error in AddFields.current_timestamp: {e}")
            raise
