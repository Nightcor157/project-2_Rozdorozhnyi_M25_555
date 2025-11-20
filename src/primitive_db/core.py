# src/primitive_db/core.py

from typing import Any, Dict, List

ALLOWED_TYPES = {"int", "str", "bool"}


def create_table(
    metadata: Dict[str, Any],
    table_name: str,
    columns: List[str],
) -> Dict[str, Any]:
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    parsed_columns: list[dict[str, str]] = []

    for col in columns:
        if ":" not in col:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return metadata

        name, type_name = col.split(":", 1)
        name = name.strip()
        type_name = type_name.strip()

        if type_name not in ALLOWED_TYPES:
            print(f"Некорректное значение: {col}. Попробуйте снова.")
            return metadata

        parsed_columns.append({"name": name, "type": type_name})

    has_id = any(column["name"] == "ID" for column in parsed_columns)
    if not has_id:
        parsed_columns.insert(0, {"name": "ID", "type": "int"})

    metadata[table_name] = {"columns": parsed_columns}

    columns_repr = ", ".join(
        f'{col["name"]}:{col["type"]}' for col in parsed_columns
    )
    print(
        f'Таблица "{table_name}" успешно создана '
        f"со столбцами: {columns_repr}",
    )

    return metadata


def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


def list_tables(metadata: Dict[str, Any]) -> None:
    for table_name in metadata:
        print(f"- {table_name}")

