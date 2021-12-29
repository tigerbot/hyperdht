#!/usr/bin/env bash

# Run this setup.sh script to copy the git pre-commit hook into the appropriate
# place in your git file structure. The pre-commit hook runs gofmt on all the
# staged Go files to ensure consistent source file formatting.
#
# After this script is run and the git hook is in place, the pre-commit script
# will be run automatically before each git commit.

set -e
set -u

# Determine where the repository root is.
SCRIPT_DIR="$(cd "$(dirname "$0")" && env pwd -P)"

# Determine where the .git directory is for the repository (in case its a submodule).
if [ -f $SCRIPT_DIR/.git ]; then
    # The git symbolic link in the .git ascii file is of the form:
    #
    # gitdir: <path>
    #
    # This sed command extracts <path>, which is the .git directory for rest.
    GIT_REST=$(sed "s/^[^ ]* //" $SCRIPT_DIR/.git)
else
    GIT_REST=$SCRIPT_DIR/.git
fi

# use a symbolic link so we don't have to run this script again
# if the pre-commit hook changes.
ln -snf $SCRIPT_DIR/pre-commit.sh $GIT_REST/hooks/pre-commit

