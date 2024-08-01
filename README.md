# CITS3200---National-Housing-Simulation

National Housing Simulation - mapping data points from the G-NAF and the census data sets.

## Install

Currently, two methods are supported. `poetry` is the main method for managing Python dependencies. `devbox` uses `poetry` to manage Python dependencies but provide extra isolation from your host environment and automates some of the processes when used with `direnv`. It can also manage non-Python dependencies as well without installing it on your main machine.

### Poetry

[Poetry](https://python-poetry.org/) is a Python dependency manager. Install it following the [official guide](https://python-poetry.org/docs/#installing-with-the-official-installer).

Run `poetry install` to install the required dependencies and then `poetry shell` to activate the virtual environment.

## Devbox + direnv

**Warning: `direnv` allow any arbitrary bash commands to be executed, please inspect `.envrc` before allowing direnv!**

[Devbox](https://github.com/jetify-com/devbox) is used to create isolated development shells where the dependencies are declared in `devbox.json` file and are version-locked in `devbox.lock`. Dependencies and programs installed in the shell are only accessible in the shell. It is internally powered by Nix where the list of Nix packages can be found at [Nixhub.io](https://www.nixhub.io/).

[`direnv`](https://github.com/direnv/direnv) is used to extend the current shell by loading and unloading environmental variables automatically as the user enters the current directory. This is used to activate the Devbox shell automatically using the `.envrc` file.

First, install both [Devbox](https://github.com/jetify-com/devbox) and [`direnv`](https://github.com/direnv/direnv). Then, from the top-most directory of the project, run the following command. When you `cd` into the directory, Devbox will install the required dependencies locally, run poetry to install the requirements, and activate the virtual environment automatically.

```bash
direnv allow    # Allow direnv to execute .envrc
```
