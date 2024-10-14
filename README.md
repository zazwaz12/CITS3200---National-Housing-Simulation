# Overview

Repository for National Housing Simulation.

This repository contains the code for the National Housing Simulation project in which census attributes from the Statistical Area 1 (SA1) is allocated to housing coordinates in the GNAF dataset.

The functions are stored under `nhs/` and example scripts can be found under `scripts/`.

Please see our [wiki](https://github.com/zazwaz12/CITS3200---National-Housing-Simulation/wiki/1.-Home) for details about our project.

## Installation

You can install project dependencies using either `poetry` or `devenv`. 

This project uses **Python 3.12.4**, which is automatically handled if you use the `devenv` install option. If you install via `poetry`, you may need to create an environment with the correct Python version before you can install the dependencies. To check your python version, run `python3 --version`.

### Poetry

[Poetry](https://python-poetry.org/) is a Python dependency manager that also manages dependency configurations. Install it following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer).

```bash
poetry install   # Install the dependencies from pyproject.toml
poetry shell     # Activate the virtual environment
```

#### Conda/Mamba for Python Version Management (Optional)
If you are using [Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html) or [Mamba](https://github.com/mamba-org/mamba) *and you don't have the right Python version by default*, the easiest way to get started with poetry is to use Conda/Mamba to create an environment with the right Python version before installing via poetry.

```bash
conda create -n py312 python=3.12   # or mamba create, use Conda to manage your python versions
conda activate py312                # use myenv that have the right python version
poetry install                      
poetry shell
```

### Devenv

[Devenv](https://devenv.sh/) is used to create isolated development shells where the dependencies are declared in `devenv.nix` file with input channels defined in `devenv.yaml` and are version-locked in `devenv.lock`. Dependencies and programs installed in the shell are only accessible in the shell. It is internally powered by Nix where the list of Nix packages can be found at [NixOS Packages](https://search.nixos.org/packages).

First, install `devenv` and `nix`. Then, from the top-most directory of the project, run the following command to let `devenv` install the required dependencies locally, install the requirements, and activate the virtual environment.

```bash
devenv shell
exit # Exit the devenv shell
```

#### Auto-activation with `direnv` (Optional)

**Warning: `direnv` allow the execution of any arbitrary bash code in `.envrc`, please examine `.envrc` before you proceed!**

[`direnv`](https://direnv.net/) is used to automatically activate `devenv` when you enter into the folder containing this repository. First, install it via the [official installation guide](https://direnv.net/docs/installation.html) and [hook it into your shell](https://direnv.net/docs/hook.html) (hint: run `echo $SHELL` to see what shell you are using). Then, inside the project directory where `.envrc` is in the same folder, run...

```bash
direnv allow  # allow execution of .envrc automatically
direnv disallow # stop automatically executing .envrc upon entering the project folder
```

and it will automatically activate `devenv` which will then install the necessary dependencies.
