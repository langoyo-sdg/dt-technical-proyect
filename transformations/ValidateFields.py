import logging
from pyspark.sql.functions import col, lit, when, array, array_union, concat_ws, concat
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from transformations.TranformationBase import TransformationBase

class ValidateFields(TransformationBase):
    def apply(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Validates fields in the DataFrame according to specified rules and logs validation errors.

        Args:
            df (DataFrame): The DataFrame to validate.
            params (dict): A dictionary containing validation parameters with:
                - "validations": A list of validation rules for fields. Each validation contains:
                    - "field": The name of the field to validate.
                    - "validations": A list of validation rules to apply ("notEmpty", "notNull"...).

        Returns:
            DataFrame: The input DataFrame with a new column "validation_errors" containing a list of validation errors for each row, 
            or an empty list if no errors are found.

        Raises:
            ValueError: If an unknown validation rule is encountered.
            RuntimeError: If an error occurs during the application of any rule, a RuntimeError is raised.

        """
        validations = params.get("validations", [])
        error_column_name = "validation_errors"

        try:
            # Temporary column to track errors
            df = df.withColumn(error_column_name, array())

            for validation in validations:
                field = validation["field"]
                rules = validation["validations"]

                for rule in rules:
                    if rule == "notEmpty":
                        self.logger.debug(f"Applying notEmpty validation for field: {field}")
                        df = self.not_empty(df, field, error_column_name)
                    elif rule == "notNull":
                        self.logger.debug(f"Applying notNull validation for field: {field}")
                        df = self.not_null(df, field, error_column_name)
                    else:
                        self.logger.error(f"Unknown validation rule: {rule}")
                        raise ValueError(f"Unknown validation rule: {rule}")

            # Convert the error list to a string (comma-separated) for final output
            df = df.withColumn(
                error_column_name,
                concat(lit("["), concat_ws(", ", col(error_column_name)), lit("]"))
            )

            return df

        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            raise RuntimeError(f"Error during validation: {e}")

    def not_null(self, df: DataFrame, field: str, error_column: str) -> DataFrame:
        """
        Validates that a specified field in the DataFrame is not null. If the field is null, 
        an error message is added to the error_column column.

        Args:
            df (DataFrame): The DataFrame to validate.
            field (str): The name of the field to check for null values.
            error_column (str): The name of the column where error messages will be appended if validation fails.

        Returns:
            DataFrame: The DataFrame with the updated error_column, containing the validation error message if the field is null.

        Raises:
            RuntimeError: If an error occurs during the validation process.
"""
        try:
            val_error_message = f"notNull: {field} must not be null"

            # Append error message to the list if the field is null
            error_condition = when(
                col(field).isNull(),
                array_union(col(error_column), array(lit(val_error_message)))
            ).otherwise(col(error_column))

            return df.withColumn(error_column, error_condition)

        except Exception as e:
            self.logger.error(f"Error in not_null validation for field {field}: {e}")
            raise RuntimeError(f"Error during validation: {e}")

    def not_empty(self, df: DataFrame, field: str, error_column: str) -> DataFrame:
        """
        Validates that a specified field in the DataFrame is not empty. If the field is empty, 
        an error message is added to the error_column column.

        Args:
            df (DataFrame): The DataFrame to validate.
            field (str): The name of the field to check for empty values.
            error_column (str): The name of the column where error messages will be appended if validation fails.

        Returns:
            DataFrame: The DataFrame with the updated error_column, containing the validation error message if the field is empty.

        Raises:
            RuntimeError: If an error occurs during the validation process.
        """

        try:
            val_error_message = f"notEmpty: {field} must not be empty"

            # Append error message to the list if the field is empty
            error_condition = when(
                col(field) == "",
                array_union(col(error_column), array(lit(val_error_message)))
            ).otherwise(col(error_column))

            return df.withColumn(error_column, error_condition)

        except Exception as e:
            self.logger.error(f"Error in not_empty validation for field {field}: {e}")
            raise RuntimeError(f"Error during validation: {e}")
