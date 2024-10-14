# Overview

Repository for National Housing Simulation.

This repository contains the code for the National Housing Simulation project in which census attributes from the Statistical Area 1 (SA1) is allocated to housing coordinates in the GNAF dataset.

The functions are stored under `nhs/` and example scripts can be found under `scripts/`.

The sections below are summarised from our [wiki](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/1.-Home), please refer to it for more details.

## Installing Project Dependencies
You can install project dependencies using either `poetry` or `devenv`. 

Note that this project uses **Python 3.12.4**, you won't need to worry about this if you use `devenv`.

### Poetry

Install [Poetry](https://python-poetry.org/) following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer), then run

```bash
poetry shell     # Activate the virtual environment
poetry install   # Install the dependencies from pyproject.toml
```

## Devenv
First, install [Devenv](https://devenv.sh/) and Nix following these [instructions](https://devenv.sh/getting-started/). Then, from the top-most directory of the project, run the following command

```bash
devenv shell
exit # Exit the devenv shell
```

## Usage
You can either
- Import the functions from `nhs` for use in your own Python scripts, or
- Run existing scripts under `scripts/`

The behaviour of the functions is dictated by a `configurations.yml` file described in this [section](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/5.-Configuring-the-Application).

### Usage as a Library
If you've installed `nhs` as a [library](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/2.-Installation#install-from-wheel) or your current working directory contains the `nhs/` folder (tip: run `os.getcwd()`), you can import the functions from `nhs` as follows
```python
from nhs import config
from nhs.data import (
    filter_and_join_gnaf_frames,
    randomly_assign_census_features,
    read_parquet,
    # and so forth...
)

# do something with the imports
data = read_parquet("some/path")
```

Please see our [documentation](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/7.-Functions) for the available functions you can import.

### Running Existing Scripts
The three scripts are available under the folder `scripts/`. You can run them as

```bash
poetry run python3 path/to/scripts/home_allocation.py --help  # see options
poetry run python3 path/to/scripts/home_allocation.py --census_dir some/path  # and other option flags
```

## Testing
To run the tests under `tests/`, run

```bash
poetry run pytest .
```
