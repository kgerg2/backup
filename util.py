import csv
from datetime import datetime, timedelta
import logging
from typing import Dict, Iterable, Tuple, Union


TIME_FORMAT = "%Y-%m-%d_%H.%M.%s,%f"

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

def get_file_info(file) -> Dict[str, Tuple[str, datetime, int]]:
    return {name: (hash, datetime.strptime(time, TIME_FORMAT), int(size)) for name, hash, time, size in read_csv(file)}

def write_csv(path, data: Iterable[Iterable]):
    with open(path, newline="") as f:
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)

def update_file_info(file, data: Dict[str, Tuple[str, datetime, int]]):
    write_csv(file, ((name, hash, time.strftime(TIME_FORMAT), size) for name, (hash, time, size) in data.items()))

def is_same_file(file1: Tuple[str, datetime, int], file2: Tuple[str, datetime, int]) -> bool:
    hash1, date1, size1 = file1
    hash2, date2, size2 = file2
    if hash1 != hash2:
        return False
    
    if size1 != size2:
        logging.warning(f"Két fájl azonos hash-sel de eltérő mérettel rendelkezett. ({hash1}, {size1} != {size2})")
        return False

    if abs(date1 - date2) < timedelta(microseconds=10):
        logging.warning("Két fájl azonos hash-sel és mérettel rendelkezett, de módosítási idejük eltérő. "
                        f"({hash1}, {size1}, {date1.isoformat()} != {date2.isoformat()})")
    return True

def write_checkfile(path, data):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(f"{hash}  {name}\n" for name, (hash, _, _) in data.items())