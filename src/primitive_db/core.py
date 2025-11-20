# src/primitive_db/core.py


from typing import Any, Dict, List

from src.decorators import (
    confirm_action,
    create_cacher,
    handle_db_errors,
    log_time,
)

from .constants import VALID_TYPES

_select_cache = create_cacher()


@handle_db_errors
def create_table(
    metadata: Dict[str, Any],
    table_name: str,
    columns: List[str],
) -> Dict[str, Any]:
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    parsed_columns: List[Dict[str, str]] = []

    for col in columns:
        if ":" not in col:
            raise ValueError(f"Некорректное определение столбца: {col}")

        name, type_name = col.split(":", 1)
        name = name.strip()
        type_name = type_name.strip()

        if type_name not in VALID_TYPES:
            raise ValueError(f"Некорректный тип столбца: {col}")

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


@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if table_name not in metadata:
        raise KeyError(table_name)

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


def list_tables(metadata: Dict[str, Any]) -> None:
    for table_name in metadata:
        print(f"- {table_name}")


# ---------- CRUD-операции с данными ----------


@log_time
@handle_db_errors
def insert(
    metadata: Dict[str, Any],
    table_name: str,
    values: List[Any],
    table_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if table_name not in metadata:
        raise KeyError(table_name)

    columns_meta = metadata[table_name]["columns"]
    id_values = [row.get("ID", 0) for row in table_data]
    new_id = max(id_values) + 1 if id_values else 1

    record: Dict[str, Any] = {"ID": new_id}

    data_columns = [col for col in columns_meta if col["name"] != "ID"]
    if len(values) != len(data_columns):
        raise ValueError("Некорректное количество значений для вставки.")

    for column_meta, value in zip(data_columns, values, strict=False):
        record[column_meta["name"]] = value

    table_data.append(record)
    print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
    return table_data


@log_time
@handle_db_errors
def select(
    table_name: str,
    table_data: List[Dict[str, Any]],
    where_clause: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    key: Any = (table_name, None)
    if where_clause is not None:
        column, value = next(iter(where_clause.items()))
        key = (table_name, (column, value))

    def compute() -> List[Dict[str, Any]]:
        if where_clause is None:
            return table_data
        column, value = next(iter(where_clause.items()))
        return [row for row in table_data if row.get(column) == value]

    return _select_cache(key, compute)


@handle_db_errors
def update(
    table_name: str,
    table_data: List[Dict[str, Any]],
    set_clause: Dict[str, Any],
    where_clause: Dict[str, Any],
) -> List[Dict[str, Any]]:
    set_column, set_value = next(iter(set_clause.items()))
    where_column, where_value = next(iter(where_clause.items()))

    updated_ids: List[int] = []

    for row in table_data:
        if row.get(where_column) == where_value:
            row[set_column] = set_value
            if "ID" in row:
                updated_ids.append(row["ID"])

    for row_id in updated_ids:
        print(
            f'Запись с ID={row_id} в таблице "{table_name}" успешно обновлена.',
        )

    return table_data


@confirm_action("удаление записей")
@handle_db_errors
def delete(
    table_name: str,
    table_data: List[Dict[str, Any]],
    where_clause: Dict[str, Any],
) -> List[Dict[str, Any]]:
    where_column, where_value = next(iter(where_clause.items()))

    remaining: List[Dict[str, Any]] = []
    deleted_ids: List[int] = []

    for row in table_data:
        if row.get(where_column) == where_value:
            if "ID" in row:
                deleted_ids.append(row["ID"])
        else:
            remaining.append(row)

    for row_id in deleted_ids:
        print(
            f'Запись с ID={row_id} успешно удалена из таблицы "{table_name}".',
        )

    return remaining


@handle_db_errors
def info_table(
    metadata: Dict[str, Any],
    table_name: str,
    table_data: List[Dict[str, Any]],
) -> None:
    if table_name not in metadata:
        raise KeyError(table_name)

    columns_meta = metadata[table_name]["columns"]
    columns_repr = ", ".join(
        f'{col["name"]}:{col["type"]}' for col in columns_meta
    )

    print(f"Таблица: {table_name}")
    print(f"Столбцы: {columns_repr}")
    print(f"Количество записей: {len(table_data)}")

