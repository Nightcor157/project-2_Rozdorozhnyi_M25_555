# src/primitive_db/engine.py

import shlex

import prompt

from .core import create_table, drop_table, list_tables
from .utils import load_metadata, save_metadata

METADATA_FILE = "db_meta.json"


def print_help() -> None:
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу",
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


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

        command = args[0]
        command_args = args[1:]

        metadata = load_metadata(METADATA_FILE)

        if command == "exit":
            break

        if command == "help":
            print_help()
            continue

        if command == "create_table":
            if not command_args:
                print("Некорректное значение: create_table. Попробуйте снова.")
                continue

            table_name = command_args[0]
            columns = command_args[1:]
            metadata = create_table(metadata, table_name, columns)
            save_metadata(METADATA_FILE, metadata)
            continue

        if command == "drop_table":
            if not command_args:
                print("Некорректное значение: drop_table. Попробуйте снова.")
                continue

            table_name = command_args[0]
            metadata = drop_table(metadata, table_name)
            save_metadata(METADATA_FILE, metadata)
            continue

        if command == "list_tables":
            list_tables(metadata)
            continue

        print(f"Функции {command} нет. Попробуйте снова.")

