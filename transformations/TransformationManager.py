import logging
from pyspark.sql import DataFrame
from transformations.AddFields import AddFields
from transformations.ValidateFields import ValidateFields

class TransformationManager:
    """
    A manager class responsible for applying various transformations to a DataFrame based on the provided metadata.

    Attributes:
        transformation_classes (dict): A dictionary mapping transformation types in metadata to their respective classes with the implementation.
        logger (logging): A logger for logging transformation activities.

    Methods:
        __init__():
            Initializes the manager with a mapping of transformation types to classes.
        apply_transformations(df: DataFrame, transformations: dict, input: str) -> DataFrame:
            Applies a series of transformations to the DataFrame based on metadata.

    """
    def __init__(self):
        """
        Initialize the manager with a mapping of transformation types to classes. Expand with new implementations.
        """
        self.transformation_classes = {
            "validate_fields": ValidateFields,
            "add_fields": AddFields,
        }
        self.logger = logging.getLogger(self.__class__.__name__) 

    def apply_transformations(self, df: DataFrame, transformations: dict, input_name: str) -> DataFrame:
        """
        Applies transformations to the DataFrame based on the provided metadata.

        Args:
            df (DataFrame): The DataFrame to which transformations will be applied.
            transformations (dict): A list of transformation configurations, where each configuration includes:
                - "input": The name of the input source.
                - "type": The type of transformation to apply (e.g., "validate_fields", "add_fields").
                - "params": Parameters for the transformation.
            input (str): The name of the input source for which transformations should be applied.

        Returns:
            DataFrame: The DataFrame after all transformations have been applied.

        Raises:
            ValueError: If an unknown transformation type is encountered.
            RuntimeError: If an error occurs during the transformation process.

        """
        for transformation in transformations:
            if transformation['input'] == input_name:
                transform_type = transformation["type"]
                params = transformation["params"]

                try:
                    if transform_type in self.transformation_classes:
                        # Dynamically instantiate the transformation class
                        transformer = self.transformation_classes[transform_type]()
                        self.logger.info(f"Applying transformation: {transform_type} with params: {params}")
                        df = transformer.apply(df, params)
                    else:
                        error_message = f"Unknown transformation rule: {transform_type}"
                        self.logger.error(error_message)
                        raise ValueError(error_message)
                except Exception as e:
                    self.logger.error(f"Error applying transformation {transform_type}: {e}")
                    raise

        self.logger.info("All transformations were applied successfully.")
        return df
