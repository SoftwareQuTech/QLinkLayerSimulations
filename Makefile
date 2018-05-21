PYTHON        = python3
SOURCEDIR     = source

tests:
	$(PYTHON) -m unittest discover -s $(SOURCEDIR) tests

.PHONY: tests
