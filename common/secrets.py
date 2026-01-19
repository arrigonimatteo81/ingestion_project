from abc import ABC, abstractmethod
from typing import Optional

from common.utils import extract_field_from_file


class SecretRetriever(ABC):
    """
    Abstract base class defining the interface for secret retrieval.

    This class defines a common interface for retrieving secrets from various
    storage mechanisms. Concrete implementations should handle the specifics
    of retrieving secrets from their respective storage backends.
    """

    @abstractmethod
    def retrieve_secret(self, path: str) -> str:
        """
        Retrieve the value of a secret given its path.

        Args:
            path (str): The path or identifier of the secret to retrieve.

        Returns:
            Optional[str]: The value of the secret if found, None otherwise.
        """
        pass


class FileSecretRetriever(SecretRetriever):
    """
    Concrete implementation of SecretRetriever that retrieves secrets from a file.

    This class reads secrets from a JSON file where the keys in the JSON structure
    correspond to the paths of the secrets.
    """

    def __init__(self, file_path: str):
        """
        Initialize a FileSecretRetriever with the path to the secrets file.

        Args:
            file_path (str): Path to the file containing the secrets.
        """
        self.file_path = file_path

    def __repr__(self):
        return f"FileSecretRetriever(file_path={self.file_path!r})"

    def retrieve_secret(self, path: str) -> str:
        """
        Retrieve a secret from the file given its path.

        Args:
            path (str): The path of the secret within the JSON structure.

        Returns:
            Optional[str]: The value of the secret if found, None otherwise.
        """
        return extract_field_from_file(self.file_path, path)


class EnvironmentVariablesSecretRetriever(SecretRetriever):
    """
    Concrete implementation of SecretRetriever that retrieves secrets from environment variables.

    This class looks up secrets in the system's environment variables.
    """

    def __init__(self, prefix: str = ""):
        raise NotImplementedError("EnvironmentVariablesSecretRetriever is not yet implemented.")

    def retrieve_secret(self, path: str) -> str:
        pass


class VaultSecretRetriever(SecretRetriever):
    """
    Concrete implementation of SecretRetriever that retrieves secrets from Hashicorp Vault.
    """

    def __init__(self):
        raise NotImplementedError("VaultSecretRetriever is not yet implemented.")

    def retrieve_secret(self, path: str) -> str:
        pass


class SecretRetrieverFactory:
    """
    Factory class to create specific SecretRetriever implementations.

    This class provides static methods to create different types of secret retrievers
    based on the needed configuration.
    """

    @staticmethod
    def create(retriever_type: str, **kwargs) -> SecretRetriever:
        """
        Create a specific SecretRetriever based on the provided type and arguments.

        Args:
            retriever_type (str): The type of secret retriever to create.
                                  Supported values: 'file', 'env'
            **kwargs: Additional arguments needed for the specific retriever type.
                      - For 'file': file_path (str)
                      - For 'env': prefix (Optional[str])

        Returns:
            SecretRetriever: An instance of the requested secret retriever.

        Raises:
            ValueError: If an unsupported retriever type is requested or if
                        required arguments are missing.
        """
        if retriever_type.lower() == 'file':
            if 'file_path' not in kwargs:
                raise ValueError("file_path is required for FileSecretRetriever")
            return FileSecretRetriever(kwargs['file_path'])

        elif retriever_type.lower() == 'env':
            return EnvironmentVariablesSecretRetriever(
                kwargs.get('prefix', '')  # prefix is optional
            )

        elif retriever_type.lower() == 'vault':
            return VaultSecretRetriever()

        else:
            raise ValueError(f"Unsupported retriever type: {retriever_type}")

    @staticmethod
    def from_dict(secret_retriever_conf: dict) -> SecretRetriever:
        """
                Create a specific SecretRetriever based on the provided configuration

                Args:
                    secret_retriever_conf (dic): Configuration of the secret retriever object
                Returns:
                    SecretRetriever: An instance of the requested secret retriever.

                Raises:
                    ValueError: If an unsupported retriever type is requested or if
                                required arguments are missing.
                """
        if secret_retriever_conf.get("type").lower() == 'file':
            return SecretRetrieverFactory.create(retriever_type="file",
                                                 file_path=secret_retriever_conf.get("file_path"))
        elif secret_retriever_conf.get("type").lower() == 'env':
            return SecretRetrieverFactory.create(retriever_type="env")
        elif secret_retriever_conf.get("type").lower() == 'vault':
            return SecretRetrieverFactory.create(retriever_type="vault")
