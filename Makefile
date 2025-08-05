# Makefile

# Python invocation settings
PYTHON := python
FLAGS  := -m

# Declare phony targets
.PHONY: help all set_up loop_tests select_tests

# Default goal when no target is provided
.DEFAULT_GOAL := help

# Help/usage information
help:
	@echo "Usage: make <target> [args]"
	@echo "Targets:"
	@echo "  set_up                   # reset and setup DB schema/triggers"
	@echo "  loop_tests <n>           # run all benchmarks from 1 to n "
	@echo "  select_tests <ids...>    # run benchmarks on selected test IDs"
	@echo "  all <n>                  # set up DB, then loop_tests with <n>"
	@echo

# Default 'all' that chains set_up, loop_tests
all: set_up loop_tests

# Capture arguments passed after the target name
ARGS := $(wordlist 2, $(words $(MAKECMDGOALS)), $(MAKECMDGOALS))

# Reset and initialize the database
set_up:
	@source .venv/bin/activate
	@echo "→ Setting up database schema..."
	@$(PYTHON) $(FLAGS) scripts.setup_db \
	&& echo "✔ Database setup completed."

# Run benchmarks in a loop, passing along the argument (number of iterations)
loop_tests:
	@echo "→ Running loop_tests with args: $(ARGS)"
	@$(PYTHON) $(FLAGS) scripts.bench_tests loop_tests $(ARGS) \
	&& echo "✔ Looping tests completed."

# Run benchmarks on selected tests, passing IDs or other filters
select_tests:
	@echo "→ Running select_tests with args: $(ARGS)"
	@$(PYTHON) $(FLAGS) scripts.bench_tests select_tests $(ARGS) \
	&& echo "✔ Selected tests completed."

# Catch-all to prevent Make from trying to build files named by args
%:
	@:
