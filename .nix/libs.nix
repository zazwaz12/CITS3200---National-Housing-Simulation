# Essential packages for python modules, path fixed by fix-python
let pkgs = import (builtins.getFlake "nixpkgs") { };
in with pkgs; [
  # Fundamental to most python packages
  gcc.cc
  glibc
  zlib
]
