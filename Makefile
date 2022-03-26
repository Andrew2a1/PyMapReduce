APP_PATH = .

lint:
	python -W ignore -m autoflake --in-place --recursive --ignore-init-module-imports --remove-duplicate-keys --remove-unused-variables --remove-all-unused-imports $(APP_PATH)
	python -m black $(APP_PATH)
	python -m isort $(APP_PATH)
	python -m mypy $(APP_PATH) --ignore-missing-imports

master:
	python ./communication/master.py

worker:
	python ./communication/worker.py