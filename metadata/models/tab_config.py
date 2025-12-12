from dataclasses import dataclass


@dataclass
class TabConfig:
    config_name: str
    config_value: str
    def __str__(self):
        return f"TabConfig(config_name={self.config_name},config_value={self.config_name})"