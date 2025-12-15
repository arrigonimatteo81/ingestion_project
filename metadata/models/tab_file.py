from dataclasses import dataclass

from processor.domain import FileFormat


@dataclass
class TabFileSource:
    file_type: str
    path: str
    sheet: str
    csv_separator: str

    def __repr__(self):
        return (f"TabFileSource(file_type={self.file_type}, path={self.path}, sheet={self.sheet},"
                f"csv_separator={self.csv_separator})")

@dataclass
class TabFileDest:
    file_type: str
    path: str
    csv_separator: str
    overwrite: bool = False

    def __repr__(self):
        return f"TabFileDest(file_type={self.file_type}, path={self.path},overwrite={self.overwrite}, separator={self.csv_separator})"

class GCS:
    def __init__(self, format_file: FileFormat, gcs_path: str):
        self.format_file = format_file
        self.gcs_path = gcs_path