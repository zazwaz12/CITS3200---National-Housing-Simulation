# Overview

Repository for National Housing Simulation.

This repository contains the code for the National Housing Simulation project in which census attributes from the Statistical Area 1 (SA1) is allocated to housing coordinates in the GNAF dataset.

The functions are stored under `nhs/` and example scripts can be found under `scripts/`.

## Install

You can install project dependencies using either `poetry` or `devenv`.

### Poetry

[Poetry](https://python-poetry.org/) is a Python dependency manager that also manages dependency configurations. Install it following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer).

```bash
poetry shell     # Activate the virtual environment
poetry install   # Install the dependencies from pyproject.toml
```

## Devenv

[Devenv](https://devenv.sh/) is used to create isolated development shells where the dependencies are declared in `devenv.nix` file with input channels defined in `devenv.yaml` and are version-locked in `devenv.lock`. Dependencies and programs installed in the shell are only accessible in the shell. It is internally powered by Nix where the list of Nix packages can be found at [NixOS Packages](https://search.nixos.org/packages).

First, install `devenv` and `nix`. Then, from the top-most directory of the project, run the following command to let `devenv` install the required dependencies locally, install the requirements, and activate the virtual environment.

```bash
devenv shell
exit # Exit the devenv shell
```

### Auto-activation with `direnv` (Optional)

**Warning: `direnv` allow the execution of any arbitrary bash code in `.envrc`, please examine `.envrc` before you proceed!**

[`direnv`](https://direnv.net/) is used to automatically activate `devenv` when you enter into the folder containing this repository. First, install it via the [official installation guide](https://direnv.net/docs/installation.html) and [hook it into your shell](https://direnv.net/docs/hook.html) (hint: run `echo $SHELL` to see what shell you are using). Then, inside the project directory where `.envrc` is in the same folder, run...

```bash
direnv allow  # allow execution of .envrc automatically
direnv disallow # stop automatically executing .envrc upon entering the project folder
```

and it will automatically activate `devenv` which will then install the necessary dependencies.
