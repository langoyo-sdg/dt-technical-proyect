# DT technical test project

The **DataFlow** project is a data engineering pipeline built using **Apache Spark** and **PySpark**. It focuses on ingesting, transforming, and writing data based on metadata-driven configurations, allowing flexibility in processing data from various sources to sinks with customizable transformations.

## Table of Contents
1. [Project Overview](#project-overview)
3. [Project Structure](#project-structure)
4. [Requirements](#requirements)
7. [Running the Pipeline](#running-the-pipeline)
7. [Metadata Explanation](#metadata-explanation)


## Project Overview

The **DataFlow** pipeline allows for:
- **Data Ingestion**: Read data from multiple input sources (JSON, CSV, etc.).
- **Transformation**: Apply a series of transformations based on a configuration (e.g., validation, adding fields).
- **Data Output**: Write the processed data to different sinks, such as files in various formats (e.g., CSV, JSON, Parquet).

The pipeline uses a **metadata configuration file** to define the structure of the data sources, transformations, and sinks. This makes the pipeline dynamic and adaptable to various data processing requirements.


## Project Structure

The project is organized as follows:

### Key Components

- **dataflow.py**: The main script that loads the metadata, initializes the readers, transformations, and writers, and runs the pipeline.
- **dataflow.ipynb**: The same as the script but in notebook.
- **tests.ipynb**: Noteboook with some tests performed.
- **Reader.py**: Responsible for loading data from various sources based on the configuration.
- **TransformationManager.py**: Contains the logic to apply transformations like validation and adding new fields.
- **Writer.py**: Responsible for writing the processed data to sinks (e.g., CSV, JSON).
- **metadata/**: Contains some examples of configuration files.
- **logs/**: Contains some log files from previous executions.
- **data/**: Contains input and output data files.


## Requirements

To run the **DataFlow** pipeline, I used the following dependencies:

- Python 3.10.11
- Java 1.8
- Spark 3.5.3
- Scala 2.12.18
- hadoop 3.5.5

### Libraries
- Install the requirements file I used in my environment
  ```bash
  pip install -r requirements.txt


# Running the Pipeline

To run the pipeline, use the `dataflow.py` script. You must specify the metadata configuration file and optionally a log file.

## Command-Line Arguments

- `-m` or `--metadata`: Path to the metadata configuration file (e.g., `./metadata/conf.json`).
- `-l` or `--logs`: Path to the log file where the logs will be saved (default: `./logs/application_logs.txt`).

## Example

Run the pipeline with the following command:

```bash
python dataflow.py -m ./metadata/conf.json -l ./logs/application_logs.txt
```

## Metadata Configuration (JSON)

```json
{
    "name": "prueba-acceso",
    "sources": [
      {
        "name": "person_inputs",
        "paths": [
          "./data/input/person/people_1.json",
          "./data/input/person/people_2.json"
        ],
        "format": "JSON",
        "schema": [
          {"field": "name", "type": "STRING"},
          {"field": "age", "type": "INTEGER"},
          {"field": "office", "type": "STRING"}
        ],
        "options": {
          "dropFieldIfAllNull": false
        }
      }
    ],
    "transformations": [
      {
        "name": "validation",
        "type": "validate_fields",
        "input": "person_inputs",
        "params": {
          "validations": [
            {
              "field": "office",
              "validations": ["notEmpty"]
            },
            {
              "field": "age",
              "validations": ["notNull"]
            }
          ]
        }
      },
      {
        "name": "ok_with_date",
        "type": "add_fields",
        "input": "person_inputs",
        "params": {
          "addFields": [
            {
              "name": "dt",
              "function": "current_timestamp"
            }
          ]
        }
      }
    ],
    "sinks": [
      {
        "input": "person_inputs",
        "name": "ok",
        "paths": [
          "./data/output/ok/person"
        ],
        "format": "csv",
        "saveMode": "OVERWRITE"
      },
      {
        "input": "person_inputs",
        "name": "ko",
        "paths": [
          "./data/output/ko/person"
        ],
        "format": "json",
        "saveMode": "OVERWRITE"
      }
    ]
}
```
## Metadata Explanation

The **metadata configuration** is a JSON file that defines the structure of the data pipeline, including input sources, data transformations, and output sinks. Each section is broken down as follows:

### 1. **sources**
The **sources** section defines the input data sources, including file paths, data format, schema, and additional options.

- **name** (String):
  - The name of the source, used to reference this data source in transformations.

- **paths** (Array of Strings):
  - A list of file paths where the data files are located. 
- **format** (String):
  - Specifies the format of the input data [`"JSON"`, `"CSV"`, `"PARQUET"`...]. 

- **schema** (Array of Objects):
  - Defines the structure of the input data. Each field has a name and a type.
  - **field** (String): The name of the field in the data (e.g., `"name"`).
  - **type** (String): The data type of the field ()"STRING":
            "TIMESTAMP":
            "FLOAT":
            "INT": 
            "INTEGER":
            "BOOLEAN":
            "BINARY":
            "SHORT":
            "BYTE":
            "DOUBLE"
            "DATE"
            "LONG"
  - **Example**:
    ```json
    [
      {"field": "name", "type": "STRING"},
      {"field": "age", "type": "INTEGER"},
      {"field": "office", "type": "STRING"}
    ]
    ```

- **options** (Object):
  - Additional spark configuration options reading the source data.
  - **Example**: `{"dropFieldIfAllNull": false}`

### 2. **transformations** (Array of Objects)
The **transformations** section specifies operations to be applied to the source data, such as validation or adding new fields.

- **name** (String):
  - The name of the transformation.
  - **Example**: `"validation"`

- **type** (String):
  - The type of transformation to apply (e.g., `validate_fields`, `add_fields`).
  - **Example**: `"validate_fields"`

- **input** (String):
  - The name of the source or previously transformed data to apply the transformation to.
  - **Example**: `"person_inputs"`

- **params** (Object):
  - Parameters specific to the transformation:
    - **For validation transformations** (`validate_fields`), the parameters include:
      - **validations**: List of field validation rules.
        - Each validation rule has:
          - **field** (String): The name of the field to validate (e.g., `"office"`).
          - **validations** (Array of Strings): List of validation functions (e.g., `"notEmpty"`, `"notNull"`).
      - **Example**:
        ```json
        {
          "validations": [
            {"field": "office", "validations": ["notEmpty"]},
            {"field": "age", "validations": ["notNull"]}
          ]
        }
        ```

    - **For adding fields** (`add_fields`), the parameters include:
      - **addFields** (Array of Objects): A list of new fields to add to the data.
        - Each field includes:
          - **name** (String): The name of the new field (e.g., `"dt"`).
          - **function** (String): The function to compute the field value (e.g., `"current_timestamp"`).
      - **Example**:
        ```json
        {
          "addFields": [
            {"name": "dt", "function": "current_timestamp"}
          ]
        }
        ```

### 3. **sinks** (Array of Objects)
The **sinks** section defines where the transformed data will be written. Each sink object includes:

- **input** (String):
  - The name of the source or transformed data to write to the sink.
- **name** (String):
  - The name of the sink, which will be used to name output files or directories.
- **type** (String):
  - The type of the sink will determine how the dataframe is stored. 
    - `ok`: Writes the records that passed the validations.
    - `ko`: Writes the records that did not passed the validatiosn.
    - `all`: Writes all the recrods.

- **paths** (Array of Strings):
  - A list of file paths to write the processed data. This can include directories or filenames.
  - **Example**: `["./data/output/ok/person"]`

- **format** (String):
  - The format in which to write the output data. Common formats include `CSV`, `JSON`, `PARQUET`...

- **saveMode** (String):
  - Specifies how to save the data (e.g. `OVERWRITE`, `APPEND`).
