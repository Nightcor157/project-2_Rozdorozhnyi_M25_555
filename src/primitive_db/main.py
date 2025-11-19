#!/usr/bin/env python3

# src/primitive_db/main.py

from .engine import welcome


def main() -> None:
    """Точка входа в консольное приложение primitive_db."""
    print("Первая попытка запустить проект!")
    welcome()


if __name__ == "__main__":
    main()

