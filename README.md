
# AB Test Evaluator


### Installation

- Make sure you have python 3 installed. Python 2 is not supported
- Run `pip install -r requirements.txt` to install the package requirements.

### Usage

- `run_import.py` is used to import a new test, and it takes a **config file** and a **event-level CSV file**.
  - Running `python run_import.py` will run with the default config and CSV files: `sample_config.yml` and `sample_data.csv`.
  - To run with your own config and CSV files, run `python run_import.py --config PATH_TO_CONFIG_FILE --csv PATH_TO_CSV_FILE`
  - Run `python run_import.py -h` to see more info on usage.
- `dash_server.py` is used to run the dash server.

#### Config File Format

- See `sample_config.yml` for an example of the config file format. More generally, follow this format:
```yaml
test_name: The name of the test
description: The description of the test

date_field: The name of the date column in the CSV file. If it's named DT, you can omit this
test_cell_field: The name of the test cell column in the CSV file. If it's named TEST_CELL, you can omit this

metrics:
  [name of metric 1]:
    type: either "continuous" or "binary"
    function: |
      If the metric is continuous, just put the name of the column to use.
      If it's binary, use the format [numerator column] / [denominator column]
  [name of metric 2]:
    ...
```    

#### CSV File Format

The CSV file should be an **event-level** dataset so the continuous metrics will calculate properly. In addition to the metrics, the CSV file should include a single date column and a single test cell column.

#### TO-DOs
* clean-up repo - move configs to directory, move dash_server.py to app directory, create assets directory for CSS, images (@mschulte)
* make button to 'callback' run_import.py - i.e run it on-demand (@mschulte)
* fix errors/bugs in dash (@mschulte)
* add bayesian eval to stats.py (@dalkheraiji)
* investigate file_upload functionality - upload csv, config to run_import.py on (@apope)
* or set-up w/ sql...or both (@apope)
* containerize this (@apope)
