# concurrency-bench

A Python-based toolkit that helps decide between threading, multiprocessing, and async paradigms by benchmarking their performance on various tests and database operations.

## Features

* **Flexible Test Harness**: Define and run custom test functions (in `scripts/tests.py`) under varying concurrency levels.
* **Database Setup & Teardown**: Automatically reset and initialize a PostgreSQL database schema and triggers via SQL scripts in the `SQL/` directory.
* **Benchmark Runner**: `bench_all` function in `scripts/bench_tests.py` executes all tests, collecting timing and success/error metrics.
* **Command-Line & Makefile Integration**: Convenient targets for setup and execution:

  * `make set_up` — reset and seed the database
  * `make loop_tests <n>` — run all tests in a loop `<n>` times
  * `make select_tests <ids...>` — run specific test IDs
  * `make all <n>` — run setup, loop, and select in one command
* **Configurable Iterations & Sleep**: Control benchmarking intensity and pacing through parameters.

## Prerequisites

* Python 3.13+
* PostgreSQL server (accessible via `psycopg2`)
* Git

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/alekseikud/concurrency-bench.git
   cd concurrency-bench
   ```

2. **Create a virtual environment and install dependencies**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## Usage

### 1. Database Setup

Run the `set_up` target to drop existing tables, recreate the schema, and install triggers:

```bash
make set_up
```

### 2. Run All Benchmarks in a Loop

Execute all test functions repeatedly to gather performance metrics. Pass the number of iterations:

```bash
make loop_tests 5    # runs 5 loops
```

### 3. Run Selected Tests

Specify one or more test IDs (or names) to benchmark specific cases:

```bash
make select_tests 1 5 6 4
```

### 4. Full Workflow

Combine setup and benchmarking in one step:

```bash
make all 3   # sets up DB, then runs make loop_tests 3
```

## Project Structure

```
concurrency-bench/
├── datasets/
│   ├── customers-100000.csv
│   ├── leads-100000.csv
│   ├── organizations-100000.csv
│   └── products-100000.csv
├── LICENSE
├── Makefile
├── README.md
├── requirements.txt
├── reports/
│   ├── normalised_table.csv
│   └── rank_test_types.csv
├── scripts/
│   ├── bench_tests.py
│   ├── report.py
│   ├── setup_db.py
│   └── tests.py
├── SQL/
│   ├── create_tables.sql
│   └── trigger.sql
```

*Excluded:* `test.txt`, `__pycache__` directories/files are not shown.

## Writing New Tests

1. Add a function in `scripts/tests.py`. Decorate or register it if necessary.
2. Update `bench_all` in `scripts/bench_tests.py` to include your new test.
3. Rerun `make loop_tests` or call the function directly:

   ```bash
   python main.py --iterations 1
   ```

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/foo`).
3. Commit your changes (`git commit -am 'Add some foo'`).
4. Push to the branch (`git push origin feature/foo`).
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

