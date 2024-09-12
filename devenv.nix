{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: {
  # https://devenv.sh/basics/
  env = {
    SHELL = "zsh";
    RCLONE_CONFIG = "./.rclone/rclone.conf";
    RCLONE_DATA_FOLDER = "./DataFiles";
  };

  # https://devenv.sh/packages/
  packages = with pkgs; [git alejandra rclone];

  # https://devenv.sh/scripts/
  scripts = {
    "python".exec = "poetry run python3";
    "python3".exec = "poetry run python3";
    "py3".exec = "poetry run python3";
    "py".exec = "poetry run python3";
    "pytest".exec = "poetry run pytest";
    "pyright".exec = "poetry run pyright";
    "mount-remote".exec = "remoteName=$(head -n 1 ./.rclone/rclone.conf | sed 's/^\\[\\(.*\\)\\]$/\\1/') && rclone --vfs-cache-mode writes mount $remoteName:/DataFiles $RCLONE_DATA_FOLDER";
  };

  enterShell = ''
    poetry --version
    python --version
    alejandra . &> /dev/null

    poetry install

    // Make rclone config file and DataFiles folder if they don't exist
    if [ ! -f \"./.rclone/rclone.conf\" ]; then
        mkdir -p \"./.rclone\"
        touch \"./.rclone/rclone.conf\"
        mkdir $RCLONE_DATA_FOLDER
        RED=\"\\033[0;31m\"
        echo -e \"\n''${RED}WARNING: New rclone config file and ''${RCLONE_DATA_FOLDER} created, please do the following:\n - Specify remote for shared DataFiles folder with\n\t'rclone config'\n   Note that /DataFiles must be in the top-most directory of OneDrive.\n - Mount remote with\n\t'devbox run mount-remote'\n\"
    fi
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep "2.42.0"
  '';

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/languages/
  languages.nix.enable = true;

  languages.python = {
    enable = true;
    version = "3.12.4";

    poetry = {
      enable = true;
      install = {
        enable = true;
        installRootPackage = false;
        onlyInstallRootPackage = false;
        compile = false;
        quiet = false;
        groups = [];
        ignoredGroups = [];
        onlyGroups = [];
        extras = [];
        allExtras = false;
        verbosity = "no";
      };
    };
  };
  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";

  # See full reference at https://devenv.sh/reference/options/
}
