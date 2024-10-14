# Overview

Repository for National Housing Simulation.

This repository contains the code for the National Housing Simulation project in which census attributes from the Statistical Area 1 (SA1) is allocated to housing coordinates in the GNAF dataset.

The functions are stored under `nhs/` and example scripts can be found under `scripts/`.

Please see our [wiki](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/1.-Home) for details about our project.

## Installing Project Dependencies
This is a summary of this [wiki article](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/2.-Installation/), please refer to that page for more details.

You can install project dependencies using either `poetry` or `devenv`. 

Note that this project uses **Python 3.12.4**, you won't need to worry about this if you use `devenv`.

### Poetry

Install [Poetry](https://python-poetry.org/) following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer), then run

```bash
poetry shell     # Activate the virtual environment
poetry install   # Install the dependencies from pyproject.toml
```

## Devenv

 is used to create isolated development shells where the dependencies are declared in `devenv.nix` file with input channels defined in `devenv.yaml` and are version-locked in `devenv.lock`. Dependencies and programs installed in the shell are only accessible in the shell. It is internally powered by Nix where the list of Nix packages can be found at [NixOS Packages](https://search.nixos.org/packages).

First, install [Devenv](https://devenv.sh/) and Nix following these [instructions](https://devenv.sh/getting-started/). Then, from the top-most directory of the project, run the following command

```bash
devenv shell
exit # Exit the devenv shell
```
