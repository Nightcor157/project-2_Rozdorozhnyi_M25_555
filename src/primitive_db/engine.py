# src/primitive_db/engine.py


import shlex

import prompt
from prettytable import PrettyTable

from .constants import META_FILE
from .core import (
    create_table,
    delete,
    drop_table,
    info_table,
    insert,
    list_tables,
    select,
    update,
)
from .parser import parse_condition, parse_values
from .utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def print_help() -> None:
    print("\n***Операции с данными***")
    print("Функции:")
    print(
        "<command> insert into <имя_таблицы> values "
        "(<значение1>, <значение2>, ...) - создать запись.",
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение> "
        "- прочитать записи по условию.",
    )
    print(
        "<command> select from <имя_таблицы> "
        "- прочитать все записи.",
    )
    print(
        "<command> update <имя_таблицы> set <столбец1> = <новое_значение1> "
        "where <столбец_условия> = <значение_условия> - обновить запись.",
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение> "
        "- удалить запись.",
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print("<command> exit - выход из программы")
    print("<command> help- справочная информация\n")


def _print_select_result(metadata: dict, table_name: str, rows: list[dict]) -> None:
    """Вывести результат select с помощью PrettyTable."""
    table_meta = metadata.get(table_name)
    if not table_meta:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return

    columns_meta = table_meta.get("columns", [])
    field_names = [col["name"] for col in columns_meta]

    pretty = PrettyTable()
    pretty.field_names = field_names

    for row in rows:
        pretty.add_row([row.get(name) for name in field_names])

    print(pretty)


def run() -> None:
    print_help()

    while True:
        user_input = prompt.string("Введите команду: ")

        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        if not args:
            continue

        command_word = args[0]
        command = command_word.lower()
        metadata = load_metadata(META_FILE)

        if command == "exit":
            break

        if command == "help":
            print_help()
            continue

        # ----- управление таблицами -----
        if command == "create_table":
            if len(args) < 3:
                print(
                    "Некорректное значение: create_table. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[1]
            columns = args[2:]
            metadata = create_table(metadata, table_name, columns)
            save_metadata(META_FILE, metadata)
            continue

        if command == "list_tables":
            list_tables(metadata)
            continue

        if command == "drop_table":
            if len(args) < 2:
                print(
                    "Некорректное значение: drop_table. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[1]
            metadata = drop_table(metadata, table_name)
            save_metadata(META_FILE, metadata)
            continue

        # ----- insert into <table> values (...) -----
        if command == "insert":
            if len(args) < 4 or args[1].lower() != "into":
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            lower_input = user_input.lower()
            values_pos = lower_input.find("values")
            if values_pos == -1:
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            values_part = user_input[values_pos + len("values") :].strip()
            values = parse_values(metadata, table_name, values_part)
            if values is None:
                continue

            table_data = load_table_data(table_name)
            table_data = insert(metadata, table_name, values, table_data)
            save_table_data(table_name, table_data)
            continue

        # ----- select from <table> [where ...] -----
        if command == "select":
            if len(args) < 3 or args[1].lower() != "from":
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)

            lower_input = user_input.lower()
            where_pos = lower_input.find("where")
            if where_pos == -1:
                rows = select(table_name, table_data)
            else:
                condition_str = user_input[where_pos + len("where") :].strip()
                where_clause = parse_condition(
                    metadata,
                    table_name,
                    condition_str,
                )
                if where_clause is None:
                    continue
                rows = select(table_name, table_data, where_clause)

            _print_select_result(metadata, table_name, rows)
            continue

        # ----- update <table> set ... where ... -----
        if command == "update":
            if len(args) < 2:
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[1]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            lower_input = user_input.lower()
            set_pos = lower_input.find("set")
            where_pos = lower_input.find("where")

            if set_pos == -1 or where_pos == -1 or where_pos < set_pos:
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            set_str = user_input[set_pos + len("set") : where_pos].strip()
            where_str = user_input[where_pos + len("where") :].strip()

            set_clause = parse_condition(metadata, table_name, set_str)
            if set_clause is None:
                continue

            where_clause = parse_condition(metadata, table_name, where_str)
            if where_clause is None:
                continue

            table_data = load_table_data(table_name)
            table_data = update(table_name, table_data, set_clause, where_clause)
            save_table_data(table_name, table_data)
            continue

        # ----- delete from <table> where ... -----
        if command == "delete":
            if len(args) < 4 or args[1].lower() != "from":
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[2]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            lower_input = user_input.lower()
            where_pos = lower_input.find("where")
            if where_pos == -1:
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            where_str = user_input[where_pos + len("where") :].strip()
            where_clause = parse_condition(metadata, table_name, where_str)
            if where_clause is None:
                continue

            table_data = load_table_data(table_name)
            table_data = delete(table_name, table_data, where_clause)
            save_table_data(table_name, table_data)
            continue

        # ----- info <table> -----
        if command == "info":
            if len(args) < 2:
                print(
                    f"Некорректное значение: {user_input}. "
                    "Попробуйте снова.",
                )
                continue

            table_name = args[1]
            table_data = load_table_data(table_name)
            info_table(metadata, table_name, table_data)
            continue

        # ----- неизвестная команда -----
        print(f"Функции {command_word} нет. Попробуйте снова.")

