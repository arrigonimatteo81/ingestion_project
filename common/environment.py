from enum import Enum


class Environment(Enum):
    """
    Enum representing different environments for deployment.

    Attributes:
        SVIL: Development environment.
        TEST: Testing environment (aka System)
        PROD: Production environment.
    """

    SVIL = "svil"
    TEST = "test"
    PROD = "prod"

    def __str__(self) -> str:
        """
        Returns the string representation of the Environment value.

        Returns:
            str: The string value of the Environment instance.
        """
        return self.value

    @classmethod
    def from_string(cls, env_string: str) -> 'Environment':
        """
        Creates an Environment instance from a string in a case-insensitive way.

        Returns:
            Environment: The corresponding Environment enum instance.

        Raises:
            ValueError: If the provided string doesn't match any Environment value.
        """
        normalized_string = env_string.lower()

        for env in cls:
            if env.value == normalized_string:
                return env

        valid_values = [env.value for env in cls]
        raise ValueError(f"Invalid environment: '{env_string}'. Valid values are: {valid_values}")