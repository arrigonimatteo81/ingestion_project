#!/bin/bash

# =================================================================
# Manual Python Package Build Script
# =================================================================
#
# Description:
#   This script automates the process of building a Python wheel package
#   with a specified version number. It handles the installation of
#   requirements, temporary modification of setup.py for versioning,
#   and cleanup after the build process.
#
# Prerequisites:
#   - Python and pip installed
#   - setup.py file in the current directory
#   - requirements.txt file in the current directory
#
# Usage:
#   ./manual_build.sh [version]
#   - If version is not provided as an argument, script will prompt for it
#   Example: ./manual_build.sh 1.0.5
#
# Flow:
#   1. Takes or prompts for version number
#   2. Installs Python dependencies from requirements.txt
#   3. Creates a backup of setup.py
#   4. Modifies setup.py to use the specified version
#   5. Builds the wheel package
#   6. Restores the original setup.py
#
# Output:
#   - Builds wheel file in: ./target/diraliases/DATAPROCGCSDIR/manual_cicd/<version>/
#
# Return codes:
#   0 - Build successful
#   1 - Build failed
#
# Note:
#   The script expects setup.py to use os.environ["VERSION"] for version
#   specification and temporarily replaces it with the provided version.
# =================================================================

TARGET=./target/diraliases/DATAPROCGCSDIR/manual_cicd
rm -fR ${TARGET}/*

vers=$1
if [ -z "${vers}" ]; then
    read -p "Which version do you want to manually build? (e.g. '1.0.5') " vers
fi
echo "Trying to build version ${vers}..."
echo "Installing requirements..."
pip install -r requirements.txt
export VERSION="${vers}"
python setup.py bdist_wheel --dist-dir ${TARGET}/"${vers}"
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo "Version '${vers}' successfully built"
    exit 0
else
    echo "Something went wrong during the build"
    exit 1
fi
echo "Restoring version in setup.py"
