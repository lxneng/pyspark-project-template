help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "build - package"

all: default

default: clean build

.venv:
	if [ ! -e ".venv/bin/activate"  ] ; then python -m venv --clear .venv ; fi

deps: .venv
	. .venv/bin/activate && pip install -U -r requirements.txt -t ./src/libs

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* -r ../dist/jobs.zip .
	cd ./src && if [ -d libs ] ; then cd libs && zip -r ../../dist/libs.zip . ; fi

