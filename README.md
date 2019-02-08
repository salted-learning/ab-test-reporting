
# Installation Instructions

- Make sure you have Docker and Docker Compose installed on your system.
- In this root directory, run `docker build . -t ab_test_box`. This will build the Docker image with all required code/dependencies and name it `ab_test_box`.
- To run the program, run `docker-compose up`. This will start the Docker container.
- To connect to the UI, go to http://localhost:8050 in your browser.

Note that if you kill Docker container, all your data changes will be lost. It's an outstanding to-do to mount the database/config files/logs as volumes so the database persists across Docker "sessions".

Also note that only the Dash app works currently. The batch data update script needs to be updated to work with new data sources.

# To-Dos

- Mount the database, config file folder, and log file folder as volumes so the data persists across runs.
  - Could also mount the code as a volume to make development quicker and easier.
  - Get the data out of the git repo
- Restructure the batch `runner.py`
  - Needs to pull from a data source that's more flexible (e.g., csv files)
  - Figure out how this works within the Docker container.
    - Do we just run `docker exec` to do the batch update?
    - Do we create a web UI to import data?
    - Something else entirely?
- Script for creating a bare SQLite database
  - Shouldn't track this file in the repo since it's data, but instead have a script that creates the `.db` file if it doesn't exist.
- Refactor stats code out of flow into separate module
- Add Bayesian stats
- Multi-armed bandits
