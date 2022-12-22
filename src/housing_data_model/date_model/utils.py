import logging
from typing import Tuple, Any, Set, Dict, Optional

from pandas._libs import json
from tqdm import tqdm

Field = Tuple[str, ...]
logger = logging.getLogger(__name__)


class CleanDuplicates:
    def __init__(self, index_field: Field):
        self._index_field = index_field
        self._seen_fields: Set[Any] = set()

    def __call__(self, data: Dict[str, Any]) -> bool:
        if data:
            key = CleanDuplicates.get_value(data, self._index_field)
            if key and key not in self._seen_fields:
                self._seen_fields.add(key)
                return True
            logging.debug(f"found a duplicate entry with {self._index_field} = {key}")
        return False

    @staticmethod
    def get_value(data: Dict[str, Any], a_field: Field) -> Any:
        if data is None or not isinstance(data, dict):
            return None

        if len(a_field) == 1:
            return data.get(a_field[0])

        return CleanDuplicates.get_value(data.get(a_field[0]), a_field[1:])


def flat_map(
    in_dict: Dict[str, Any], separator: str = "/", prefix=None
) -> Dict[str, Any]:
    res = {}
    for k, v in in_dict.items():
        if prefix:
            res_key = separator.join((prefix, k))
        else:
            res_key = k

        if isinstance(v, dict):
            res.update(flat_map(v, separator, prefix=res_key))
        else:
            res[res_key] = v
    return res


def clean_data(row, required_key):
    for k in set(row.keys()):
        if k not in required_key:
            row.pop(k)
    return row


def load_from_files(files):
    for f in files:
        with open(f, "r") as fin:
            yield from fin


house_sigma_field_dtypes = {
    "ml_num": "object",
    "house_type_name": "category",
    "house_style": "category",
    "address": "object",
    "community_name": "category",
    "municipality_name": "category",
    "bedroom": "int32",
    "bedroom_plus": "int32",
    "washroom": "int32",
    "parking/total": "int32",
    "parking/garage_type": "category",
    "parking/garage": "int32",
    "parking/parking_type": "category",
    "parking/parking": "int32",
    "price_int": "int32",
    "tax_int": "int32",
    "tax_year": "int32",
    "build_year": "category",
    "price_sold_int": "int32",
    "map/lat": "float64",
    "map/lon": "float64",
    "house_area/estimate": "int32",
    "land/front": "float64",
    "land/depth": "float64",
    "date_added": "datetime64",
    "date_start": "datetime64",
    "date_update": "datetime64",
    "date_end": "datetime64",
    "list_days": "int32",
    "estimate_price_date": "datetime64",
    "estimate_price": "object",
    "scores/school": "int32",
    "scores/land": "int32",
    "scores/rent": "int32",
    "scores/growth": "int32",
    "basement": "category",
    "air_condition": "category",
    "heat_type": "category",
    "construction": "category",
    "description1": "object",
    "brokerage": "category",
}


def standardize_data(data_stream):
    def safe_load(line: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(line)
        except ValueError:
            logger.warning(f"cannot parse {line}")
            return None

    json_rows = map(safe_load, data_stream)

    # Remove invalid rows
    json_rows = filter(lambda x: x, json_rows)

    # simplify map by mixing house and analytics data
    house_data = map(
        lambda x: {**x.get("house", {}), **x.get("analytics", {})}, json_rows
    )

    filtered_rows = filter(CleanDuplicates(("ml_num",)), house_data)

    flat_rows = map(flat_map, filtered_rows)

    cleaned_rows = map(lambda r: clean_data(r, house_sigma_field_dtypes), flat_rows)
    return cleaned_rows
