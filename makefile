install:
	poetry install

project:
	poetry run project

build:
	poetry build

publish:
	poetry publish --dry-run

package-install:
	python3 -m pip install --break-system-packages --force-reinstall dist/*.whl


lint:
	poetry run ruff check .

