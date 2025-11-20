# src/primitive_db/parser.py


from typing import Any, Dict, List


def get_column_type(
    metadata: Dict[str, Any],
    table_name: str,
    column_name: str,
) -> str | None:
    table_meta = metadata.get(table_name)
    if not table_meta:
        return None

    for column in table_meta.get("columns", []):
        if column["name"] == column_name:
            return column["type"]
    return None


def convert_value(raw_value: str, type_name: str) -> Any:
    if type_name == "int":
        return int(raw_value)
    if type_name == "bool":
        lower = raw_value.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        raise ValueError
    if type_name == "str":
        if raw_value.startswith('"') and raw_value.endswith('"'):
            return raw_value[1:-1]
        if raw_value.startswith("'") and raw_value.endswith("'"):
            return raw_value[1:-1]
        return raw_value
    return raw_value


def parse_values(
    metadata: Dict[str, Any],
    table_name: str,
    values_part: str,
) -> List[Any] | None:
    values_part = values_part.strip()
    if not values_part.startswith("(") or not values_part.endswith(")"):
        print(f"Некорректное значение: {values_part}. Попробуйте снова.")
        return None

    inner = values_part[1:-1].strip()
    if not inner:
        print(f"Некорректное значение: {values_part}. Попробуйте снова.")
        return None

    raw_items = [item.strip() for item in inner.split(",")]

    table_meta = metadata.get(table_name)
    if not table_meta:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return None

    columns_meta = table_meta.get("columns", [])
    data_columns = [col for col in columns_meta if col["name"] != "ID"]

    if len(raw_items) != len(data_columns):
        print(f"Некорректное значение: {values_part}. Попробуйте снова.")
        return None

    result: List[Any] = []

    for raw_item, column_meta in zip(raw_items, data_columns, strict=False):
        type_name = column_meta["type"]
        try:
            value = convert_value(raw_item, type_name)
        except (ValueError, TypeError):
            print(f"Некорректное значение: {raw_item}. Попробуйте снова.")
            return None
        result.append(value)

    return result


def parse_condition(
    metadata: Dict[str, Any],
    table_name: str,
    condition_str: str,
) -> Dict[str, Any] | None:
    if "=" not in condition_str:
        print(f"Некорректное значение: {condition_str}. Попробуйте снова.")
        return None

    left, right = condition_str.split("=", 1)
    column_name = left.strip()
    raw_value = right.strip()

    type_name = get_column_type(metadata, table_name, column_name)
    if type_name is None:
        print(f"Некорректное значение: {column_name}. Попробуйте снова.")
        return None

    try:
        value = convert_value(raw_value, type_name)
    except (ValueError, TypeError):
        print(f"Некорректное значение: {raw_value}. Попробуйте снова.")
        return None

    return {column_name: value}

