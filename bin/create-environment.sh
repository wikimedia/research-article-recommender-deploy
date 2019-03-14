#!/usr/bin/env bash

# Creates a virtual environment zip file with packages from
# requirements.txt installed. The zip file can be used in an Oozie job.

# Adapted from
# https://github.com/wikimedia/wikimedia-discovery-analytics/blob/master/scap/checks/build_deployment_virtualenvs.sh

set -e
set -o errexit
set -o nounset
set -o pipefail

BASE_DIR="$(dirname $(dirname $(realpath $0)))"
REQUIREMENTS="${BASE_DIR}/requirements.txt"

if [ ! -f "$REQUIREMENTS" ]; then
    echo No requirements found at $REQUIREMENTS
else
    ZIP_PATH="${BASE_DIR}/article-recommender-venv.zip"
    VENV="${BASE_DIR}/venv"
    # The shebang line in linux has a limit of 128 bytes, which
    # we can overrun. Call python directly with pip to avoid shebang
    PIP="${VENV}/bin/python ${VENV}/bin/pip"

    # Ensure we have a virtualenv
    if [ ! -x "$PIP" ];then
        mkdir -p "$VENV"
        virtualenv --never-download --python python3 "$VENV"
    fi

    # Install or upgrade our packages
    $PIP install \
        -vv \
        --no-index \
        --upgrade \
        --force-reinstall \
        -r "$REQUIREMENTS"
    # Wrap it all up to be deployed by spark to executors
    ( cd "$VENV" && zip -qr "${ZIP_PATH}" . )
    rm -rf "$VENV"
    echo Saved virtualenv to $ZIP_PATH
fi

