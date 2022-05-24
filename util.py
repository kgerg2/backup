import csv
from datetime import datetime, timedelta
from typing import Dict, Iterable, Tuple, Union

class CSVDialect(csv.Dialect):
    delimiter: str = ":"
    doublequote: bool = False
    escapechar: str | None = "\\"
    lineterminator: str = "\n"
    quotechar: str | None = "\""
    quoting: csv._QuotingType = csv.QUOTE_MINIMAL
    skipinitialspace: bool = True
    strict: bool = False

def read_csv(path):
    with open(path, newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            yield line

def get_file_info(file) -> Dict[str, Tuple[datetime, int]]:
    return {name: (datetime.fromtimestamp(time), int(size)) for name, time, size in read_csv(file)}

def is_same_file(file1, file2):
    date1, size1 = file1
    date2, size2 = file2
    return abs(date1 - date2) < timedelta(microseconds=10) and size1 == size2
