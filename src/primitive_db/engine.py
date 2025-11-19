# src/primitive_db/engine.py

import prompt


def print_help() -> None:
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")


def welcome() -> None:
    print()
    print("***")
    print_help()

    while True:
        command = prompt.string("Введите команду: ").strip()

        if command == "exit":
            # Выход из программы
            break
        if command == "help":
            print()
            print_help()
            continue


