import unittest
import logging
import os
import pytest
from pandas.core.computation.common import result_type_many

from common.utils import (
    extract_field_from_file,
    get_log_level_from_file,
    string_to_dict,
    dict_to_string, get_logger, extract_db_type_from_jdbc_url
)

test_dir = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = f"{test_dir}/resources/application.conf"

DEFAULT_LOG_LEVEL = logging.INFO
ROOT_PACKAGE_NAME = "root"


class TestFunctionExtractFieldFromFile(unittest.TestCase):

    def test_function_extract_field_from_file_extracts_a_complex_object(self):
        file_path = CONF_PATH
        field = "CONNECTION_PARAMS"
        actual = extract_field_from_file(file_path, field)
        expected: dict = {"host": "localhost", "port": "port", "user": "user","password": "password","dbname": "db"}
        self.assertEqual(actual, expected)

    def test_function_extract_field_from_file_extracts_a_nested_complex_object(self):
        file_path = CONF_PATH
        field = "nested_field_with_complex_object.complex_field"
        actual = extract_field_from_file(file_path, field)
        expected: dict = {"key1": "value1", "key2": "value2"}
        self.assertEqual(actual, expected)

    def test_function_extract_field_from_file_extracts_an_array(self):
        file_path = CONF_PATH
        field = "myarray"
        actual = extract_field_from_file(file_path, field)
        expected: list = ["item1", "item2"]
        self.assertEqual(actual, expected)

    def test_function_extract_field_from_file_extracts_a_nested_array(self):
        file_path = CONF_PATH
        field = "nested_field_with_array.myarray"
        actual = extract_field_from_file(file_path, field)
        expected: list = ["item1", "item2"]
        self.assertEqual(actual, expected)

    def test_function_extract_field_from_file_with_wrong_path_raises_an_error(self):
        with self.assertRaises(FileNotFoundError) as context:
            file_path = "test_error"
            field = "CONNECTION_PARAMS"
            extract_field_from_file(file_path, field)

    def test_function_extract_field_from_file_with_wrong_field_raises_an_error(self):
        with self.assertRaises(KeyError) as context:
            file_path = CONF_PATH
            field = "test_error"
            extract_field_from_file(file_path, field)

class TestGetPackageLogLevelFromFile(unittest.TestCase):
    def test_specific_file_level(self):
        """Test for file-specific log level."""
        result = get_log_level_from_file(
            package="my_package", file="specific_file.py", file_path=CONF_PATH
        )
        self.assertEqual(result, logging.ERROR)

    def test_package_wildcard_level(self):
        """Test to check the log level for the packet with the wildcard."""
        result = get_log_level_from_file(
            package="my_package", file="another_file.py", file_path=CONF_PATH
        )
        self.assertEqual(result, logging.DEBUG)

    def test_root_level(self):
        """Test to check the log level for the root package."""
        result = get_log_level_from_file(
            package="another_package",
            file="unknown_file.py",
            file_path=CONF_PATH,
        )
        self.assertEqual(result, logging.WARN)

class TestJsonConversionFunctions(unittest.TestCase):
    def test_dict_to_string_valid(self):
        """Test dict_to_string with a valid dictionary."""
        dictionary = {"key1": "value1", "key2": 123, "key3": True}
        expected = '{"key1": "value1", "key2": 123, "key3": true}'
        self.assertEqual(dict_to_string(dictionary), expected)

    def test_dict_to_string_empty(self):
        """Test dict_to_string with an empty dictionary."""
        dictionary = {}
        expected = "{}"
        self.assertEqual(dict_to_string(dictionary), expected)

    def test_dict_to_string_invalid(self):
        """Test dict_to_string with an invalid input (non-dictionary)."""
        with self.assertRaises(Exception) as context:
            dict_to_string("not a dictionary")
        self.assertIn("Error converting dictionary to string", str(context.exception))

    def test_string_to_dict_valid(self):
        """Test string_to_dict with a valid JSON string."""
        json_string = '{"key1": "value1", "key2": 123, "key3": true}'
        expected = {"key1": "value1", "key2": 123, "key3": True}
        self.assertEqual(string_to_dict(json_string), expected)

    def test_string_to_dict_single_quotes(self):
        """Test string_to_dict with a JSON string using single quotes."""
        json_string = "{'key1': 'value1', 'key2': 123, 'key3': true}"
        expected = {"key1": "value1", "key2": 123, "key3": True}
        self.assertEqual(string_to_dict(json_string), expected)

    def test_string_to_dict_empty(self):
        json_string = "{}"
        expected = {}
        self.assertEqual(string_to_dict(json_string), expected)

    def test_string_to_dict_invalid(self):
        invalid_json_string = "{key1: value1, key2: 123}"
        with self.assertRaises(Exception) as context:
            string_to_dict(invalid_json_string)

    def test_string_to_dict_as_inverse_function_of_dict_to_string(self):
        my_dict: dict = {"key1": "value1", "key2": 123, "key3": True}
        my_dict_converted: dict = string_to_dict(dict_to_string(my_dict))
        self.assertEqual(my_dict_converted, my_dict)

    def test_dict_to_string_as_inverse_function_of_string_to_dict(self):
        mystr: str = '{"key1": "value1", "key2": 123, "key3": true}'
        mystr_converted: str = dict_to_string(string_to_dict(mystr))
        self.assertEqual(mystr, mystr_converted)

class TestGetLogger(unittest.TestCase):

    print(__name__)
    def test_log_level(self):
        result=get_logger("orchestrator.main_orchestrator.py",CONF_PATH)
        self.assertEqual(logging.ERROR,result.level)

    def test_log_level_for_non_existing_name(self):
        result=get_logger("nonexists.non_exists.py",CONF_PATH)
        self.assertEqual(logging.WARNING,result.level)

    def test_log_isEnabled_for_debug(self):
        result=get_logger("processor.main_processor.py",CONF_PATH)
        self.assertTrue(result.isEnabledFor(logging.DEBUG))

    def test_log_isEnabled_for_info(self):
        result=get_logger("processor.main_processor.py",CONF_PATH)
        self.assertTrue(result.isEnabledFor(logging.INFO))

    def test_log_isNotEnabled_for_info(self):
        result=get_logger("test_package.file.py",CONF_PATH)
        self.assertFalse(result.isEnabledFor(logging.INFO))

    def test_log_level_double_package(self):
        result=get_logger(__name__,CONF_PATH)
        self.assertEqual(logging.CRITICAL,result.level)

class TestFunctionExtractDbTypeFromJdbcUrl(unittest.TestCase):
    def test_function_extract_db_type_from_jdbc_url_extracts_oracle(self):
        res = extract_db_type_from_jdbc_url("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host.miodominio.com)(PORT=port))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME= service_name)))")
        self.assertEqual(res,"oracle")

    def test_function_extract_from_not_valid_jdbc(self):
        res = extract_db_type_from_jdbc_url("jdbc_non_valido")
        self.assertIsNone(res,"oracle")


