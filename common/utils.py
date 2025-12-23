import json
import logging
from pathlib import Path
from google.cloud import storage
import re
from typing import Union, Dict

DEFAULT_LOG_LEVEL = logging.DEBUG
ROOT_PACKAGE_NAME = "root"

def download_from_gcs(file_path: str) -> str:
    """
    Downloads a file from Google Cloud Storage (GCS) using the provided file path.

    Args:
        file_path (str): The file path in the format 'gs://bucket_name/file_name'.

    Returns:
        str: The name of the downloaded file.
    """

    storage_client = storage.Client()
    # the prefix must be "gs://"
    bucket_name = re.match(r"gs://([^/]+)/", file_path.lower()).group(1)
    bucket = storage_client.bucket(bucket_name)

    combined_uri = "gs://" + bucket_name + "/"
    # remove the prefix gs:// and the bucket name from the file_path
    result_source_blob_name = file_path.lower().replace(combined_uri, "")
    blob = bucket.blob(result_source_blob_name)

    # retrieve only the filename from the file_path
    file_path = file_path.split("/")[-1]
    blob.download_to_filename(file_path)
    return file_path

def extract_field_from_file(file_path: str, field_name: str) -> Union[Dict, str, list]:
    """
    Extracts a specified field from a JSON file.

    This function checks if the file path is a Google Cloud Storage (GCS) URL.
    If so, it downloads the file. Otherwise, it checks if the file exists at the local path.
    Once the file is opened, it loads the JSON content and returns the value of the specified field.

    :param file_path: The path of the JSON file from which to extract the field.
    :param field_name: The name of the field to extract from the JSON file.
    :raises FileNotFoundError: If the specified file does not exist.
    :raises KeyError: If the specified field is not present in the JSON file.
    :return: The value of the specified field if present in the JSON file.
    """

    # Check if the file path starts with "gs://", indicating a file on Google Cloud Storage
    if file_path.lower().startswith("gs://"):
        file_path = download_from_gcs(file_path)
    else:
        file: Path = Path(file_path)
        if not file.is_file():
            raise FileNotFoundError(
                f"The file specified at path {file_path} doesn't exist"
            )
    with open(file_path, "r") as file_json:

        parts = field_name.split('.')
        current_part = json.load(file_json)

        for part in parts:
            if part not in current_part:
                raise KeyError(f"The field {field_name} is not present in the JSON file")
            current_part = current_part[part]
        return current_part


def get_log_level_from_file(
        package: str, file: str, file_path: str = "application.conf"
):
    """
    Extracts the log level from a configuration file.

    Args:
        package (str): The name of the package for which to obtain the log level.
        file (str): The name of the file for which to obtain the log level.
        file_path (str): The path to the configuration file (default: "application.conf").

    Returns:
        int: The corresponding log level, as defined in the log level mapping.
    """

    level_mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
        "FATAL": logging.CRITICAL,
    }

    try:
        log_levels: dict = extract_field_from_file(file_path, "LOG_LEVEL")
        # Constructs the package name for logging
        log_level_processor_all = f"{package}.*"
        if log_levels.get(file) or log_levels.get(log_level_processor_all):
            level = log_levels.get(file) or log_levels.get(log_level_processor_all)
            print(f"Returning level {level_mapping.get(level)} for file {file}")
            return level_mapping.get(level)
        elif log_levels.get(ROOT_PACKAGE_NAME):
            print(
                f"Returning level {level_mapping.get(log_levels[ROOT_PACKAGE_NAME])} for package {package}, using '{ROOT_PACKAGE_NAME}' since package-level log is not defined"
            )
            return level_mapping.get(log_levels["root"])
        else:
            print(
                f"Returning level {DEFAULT_LOG_LEVEL} for package {package}, using default since package-level log is not defined"
            )
            return DEFAULT_LOG_LEVEL
    except KeyError:
        return DEFAULT_LOG_LEVEL

def get_logger(name: str, application_conf_name: str = "application.conf"):
    """
    Creates and returns a configured logger for the specified module.

    :param name: The name of the module for which to create the logger.
    :param application_conf_name: The name of the application configuration file (default: "application.conf").
    :return: A configured logger object.
    """

    from pathlib import Path

    package_and_file = name.replace(".py", "")
    parts = package_and_file.split(".")
    file = parts[-1].replace(".py", "")
    package = ".".join(parts[:-1]) if len(parts) > 1 else None

    log_format = "%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logger = logging.getLogger(name)

    if Path(application_conf_name).exists():
        level_for_current_package = get_log_level_from_file(
            package, file, application_conf_name
        )
        logger.setLevel(level_for_current_package)
        logging.basicConfig(
            format=log_format, level=level_for_current_package, datefmt=date_format
        )
    else:
        logging.basicConfig(
            format=log_format, level=DEFAULT_LOG_LEVEL, datefmt=date_format
        )
    return logger

def print_json(my_dict):
    if not isinstance(my_dict, dict):
        raise TypeError("Input must be a dictionary")
    import json

    print(json.dumps(my_dict, indent=2, ensure_ascii=False))


def dict_to_string(dict_to_convert):
    """
    Convert a dictionary to a JSON string.

    Args:
        dict_to_convert (dict): The dictionary to convert

    Returns:
        str: JSON string representation of the dictionary
    """
    try:
        if not isinstance(dict_to_convert, dict):
            raise ValueError("Input must be a dictionary")
        return json.dumps(dict_to_convert)
    except Exception as e:
        raise Exception(f"Error converting dictionary to string: {str(e)}")


def string_to_dict(str_to_convert):
    """
    Convert a JSON string to a dictionary.

    Args:
        str_to_convert (str): The JSON string to convert

    Returns:
        dict: Dictionary created from the JSON string
    """

    try:
        # Handle cases where string might use single quotes instead of double quotes
        if "'" in str_to_convert:
            str_to_convert = str_to_convert.replace("'", '"')
        return json.loads(str_to_convert)
    except Exception as e:
        raise Exception(f"Error converting string {str_to_convert} to dictionary: {str(e)}")

def extract_db_type_from_jdbc_url(jdbc_url:str):
    """

    :param jdbc_url:
    :return: the string between 'jdbc:' and the next ':'
    """
    match = parse_jdbc_url_string(jdbc_url,r"jdbc:([^:]+):")
    return match.group(1) if match else None

def parse_jdbc_url_string (jdbc_url:str, pattern:str):
    return re.search(pattern, jdbc_url, re.IGNORECASE)

