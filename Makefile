PYTHON        = python3
PIP           = pip3
SOURCEDIR     = qlinklayer
TESTS         = tests


clean:
	@find . -name '*.pyc' -delete

lint:
	@$(PYTHON) -m flake8 $(SOURCEDIR) $(TESTS)

python-deps:
	@$(PIP) install -r requirements.txt

tests:
	@$(PYTHON) -m unittest discover -s $(SOURCEDIR) $(TESTS)

verify: clean python-deps lint tests

.PHONY: clean lint python-deps tests verify
