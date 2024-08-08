# National Housing Simulation

Repository for the National Housing Simulation project, involving mapping data points from the G-NAF and the SA1 census datasets.

## Install

You can install project dependencies using either `poetry` or `devbox`.

### Poetry

[Poetry](https://python-poetry.org/) is a Python dependency manager that also manages dependency configurations. Install it following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer).

```bash
poetry shell     # Activate the virtual environment
poetry install   # Install the dependencies from pyproject.toml
```

## Devbox

[Devbox](https://github.com/jetify-com/devbox) is used to create development shells where the dependencies are declared in `devbox.json` file. It can manage non-Python dependencies which will only be accessible in the shell (i.e. not installed globally). It is internally powered by Nix where the list of Nix packages can be found at [Nixhub.io](https://www.nixhub.io/).

First, install [Devbox](https://www.jetify.com/devbox/docs/installing_devbox/). Then, from the top-most directory of the project, run

```bash
devbox shell  # activate the dev shell
```
