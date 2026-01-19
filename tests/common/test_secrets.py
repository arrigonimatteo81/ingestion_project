import os
import unittest

from common.secrets import SecretRetrieverFactory, FileSecretRetriever

test_dir = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = f"{test_dir}/resources/application.conf"
SECRETS_PATH = f"{test_dir}/resources/secrets.conf"



class TestSecretRetrieverFactory(unittest.TestCase):

    def test_create_file_retriever_with_valid_configuration(self):
        retriever = SecretRetrieverFactory.create('file', file_path=CONF_PATH)
        self.assertIsInstance(retriever, FileSecretRetriever)
        self.assertEqual(retriever.file_path, CONF_PATH)

    def test_create_file_retriever_missing_path(self):
        with self.assertRaises(ValueError) as context:
            SecretRetrieverFactory.create('file')
        self.assertEqual(str(context.exception), "file_path is required for FileSecretRetriever")

    def test_create_env_retriever_not_yet_implemented(self):
        with self.assertRaises(NotImplementedError) as context:
            SecretRetrieverFactory.create('env')
        with self.assertRaises(NotImplementedError) as context:
            SecretRetrieverFactory.create('ENV')

    def test_create_vault_retriever_not_yet_implemented(self):
        with self.assertRaises(NotImplementedError) as context:
            SecretRetrieverFactory.create('vault')
        with self.assertRaises(NotImplementedError) as context:
            SecretRetrieverFactory.create('VAULT')

    def test_create_unsupported_retriever(self):
        with self.assertRaises(ValueError) as context:
            SecretRetrieverFactory.create('unsupported_type')

        self.assertEqual(
            str(context.exception),
            "Unsupported retriever type: unsupported_type"
        )

    def test_from_dict_file_retriever_with_valid_configuration(self):
        conf: dict = {
            "type": "file",
            "file_path": CONF_PATH
        }
        retriever = SecretRetrieverFactory.from_dict(conf)
        self.assertIsInstance(retriever, FileSecretRetriever)
        self.assertEqual(retriever.file_path, CONF_PATH)


class TestFileSecretRetriever(unittest.TestCase):

    def test_file_secret_retriever_returns_secret(self):
        file_retriever: FileSecretRetriever = FileSecretRetriever(file_path=SECRETS_PATH)
        self.assertTrue(isinstance(file_retriever, FileSecretRetriever))
        result = file_retriever.retrieve_secret("secrets.user@domain")
        self.assertEqual(result, "pwd1")

    def test_file_secret_retriever_returns_key_error_when_an_invalid_secret_is_asked(self):
        with self.assertRaises(KeyError):
            file_retriever: FileSecretRetriever = FileSecretRetriever(file_path=SECRETS_PATH)
            file_retriever.retrieve_secret("secrets.invalid_secret")


if __name__ == '__main__':
    unittest.main()
