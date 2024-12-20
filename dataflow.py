# %%
import os
import sys
import json
import argparse
import logging
from pyspark.sql import SparkSession
from transformations.TransformationManager import TransformationManager
from input.Reader import Reader
from output.Writer import Writer

spark = SparkSession.builder.master("local").appName("Dataflow").getOrCreate()

if __name__ == "__main__":
    # Adding arguments: One for the metadata file and another for the logs file
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-m', '--metadata', 
        type=str, 
        required=True, 
        help="Path to the dataflow metadata configuration."
    )
    parser.add_argument(
        '-l', '--logs', 
        type=str, 
        default="./logs/application_logs.txt", 
        help="Directory to save the logs. Default is './logs/application_logs.txt'."
    )
    
    args = parser.parse_args()

    # Configure the logging system
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level
        format= "%(asctime)s - " + f"{args.metadata}" + " - %(name)s - %(levelname)s - %(message)s",  # Log message format
        handlers=[
            logging.FileHandler(args.logs),  # Dump logs to a file
            logging.StreamHandler()  # Also print logs to the console
        ]
    )

    # Reading metadata
    try:
        with open(args.metadata, 'r') as file:
            metadata = json.load(file)
    except Exception as e:
        logging.logger.error(f"Error opening the metadata file '{args.metadata}': {e}")
        raise RuntimeError(f"Error during validation: {e}")
    
    # Loading dataframes
    reader = Reader(spark)
    sources = reader.load_df(metadata['sources'])

    # Applying transformations
    transformator = TransformationManager()
    for source_name, source in sources.items():
        sources[source_name] = transformator.apply_transformations(df=source,transformations=metadata['transformations'], input_name=source_name)

    # Writing to sinks
    from output.Writer import Writer
    writer = Writer(spark_session=spark)
    writer.write_dataframes(sources, metadata['sinks'])


