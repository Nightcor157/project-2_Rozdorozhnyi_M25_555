# src/decorators.py

import time
from typing import Any, Callable, TypeVar, cast

import prompt

F = TypeVar("F", bound=Callable[..., Any])


def handle_db_errors(func: F) -> F:

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: Файл данных не найден. "
                "Возможно, база данных не инициализирована.",
            )
        except KeyError as error:
            print(f"Ошибка: Таблица или столбец {error} не найден.")
        except ValueError as error:
            print(f"Ошибка валидации: {error}")
        except Exception as error:  # noqa: BLE001
            print(f"Произошла непредвиденная ошибка: {error}")

    return cast(F, wrapper)


def confirm_action(action_name: str) -> Callable[[F], F]:

    def decorator(func: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            answer = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ',
            ).strip().lower()
            if answer != "y":
                print("Операция отменена пользователем.")
                # Возвращаем первый аргумент (метаданные или данные),
                # чтобы состояние не менялось.
                return args[0] if args else None
            return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator


def log_time(func: F) -> F:

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        result = func(*args, **kwargs)
        duration = time.monotonic() - start
        print(
            f"Функция {func.__name__} выполнилась за "
            f"{duration:.3f} секунд.",
        )
        return result

    return cast(F, wrapper)


def create_cacher() -> Callable[[Any, Callable[[], Any]], Any]:

    cache: dict[Any, Any] = {}

    def cache_result(key: Any, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result

