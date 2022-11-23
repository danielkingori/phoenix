#!/bin/bash

# All through installing in editable mode
# is maybe not the most ideal for a production
# setup, there is a number of problems when installing
# not in editable. The problem found was running of notebook from papermill as the relative path
# are not found due to the CLI using the relative path
# from the place where the package is installed rather
# then the source code or the current working directory.
# There may be other problems
pip install -e .

exec $@
