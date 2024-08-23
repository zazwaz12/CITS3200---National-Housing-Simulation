export PATH="$HOME/.local/bin:$PATH"
eval "$(pyenv virtualenv-init -)"
# pyenv configuration
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
