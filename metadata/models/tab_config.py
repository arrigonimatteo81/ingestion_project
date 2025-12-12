from dataclasses import dataclass


@dataclass
class Config:
    config_name: str
    config_value: str
    def __str__(self):
        return f"Config(config_name={self.config_name},config_value={self.config_name})"