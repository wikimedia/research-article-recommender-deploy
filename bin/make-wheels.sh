#!/usr/bin/env bash
#
# Creates wheels for article-recommender python dependencies
#
# Adapted from scripts in
# https://phabricator.wikimedia.org/diffusion/WDAN/browse/master/bin/

# Scans article_recommender oozie task for requirements.txt. collects
# all dependencies as wheel files into artifacts/article-recommender,
# then writes out a requirements-frozen.txt next to the original
# specifying exact versions to install when deploying.
#
set -e
set -o errexit
set -o nounset
set -o pipefail
# Used by wheel >= 0.25 to normalize timestamps. Timestamp
# taken from original debian patch:
# https://bugs.debian.org/cgi-bin/bugreport.cgi?att=1;bug=776026;filename=wheel_reproducible.patch;msg=5
export SOURCE_DATE_EPOCH=315576060

BASE_DIR="$(dirname $(dirname $(dirname $(realpath $0))))"
WHEEL_DIR="${BASE_DIR}/artifacts/article-recommender"
OOZIE_DIR="${BASE_DIR}/oozie"
BUILD_DIR="${BASE_DIR}/_article_recommender_build"
VENV_DIR="${BUILD_DIR}/venv"
PIP="${VENV_DIR}/bin/pip"
TASK_DIR="${OOZIE_DIR}/article_recommender"
TASK_NAME="$(basename "$TASK_DIR")"
REQUIREMENTS="${TASK_DIR}/requirements.txt"
REQUIREMENTS_FROZEN="${TASK_DIR}/requirements-frozen.txt"

if [ ! -f "${REQUIREMENTS}" ]; then
    echo "No python packaging needed for $TASK_NAME"
else
    echo "Building python packaging for $TASK_NAME"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${VENV_DIR}"
    virtualenv --python "${PYTHON_PATH:-python3}" "${VENV_DIR}"

    $PIP install -r "${REQUIREMENTS}"
    $PIP freeze --local | grep -v pkg-resources > "${REQUIREMENTS_FROZEN}"
    $PIP install pip wheel
    # Debian jessie based hosts require updated pip and wheel packages or they will
    # refuse to install some packages (numpy, scipy, maybe others)
    $PIP wheel --find-links "${WHEEL_DIR}" \
            --wheel-dir "${WHEEL_DIR}" \
            pip wheel
    $PIP wheel --find-links "${WHEEL_DIR}" \
            --wheel-dir "${WHEEL_DIR}" \
            --requirement "${REQUIREMENTS_FROZEN}"
    rm -rf "${BUILD_DIR}"
fi
