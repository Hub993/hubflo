#!/usr/bin/env bash
set -euo pipefail

stage2_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hubflo-stage2.XXXXXX")"
trap 'rm -rf "${stage2_tmp_dir}"' EXIT

stage2_python="${HUBFLO_TEST_PYTHON:-python3}"
export DATABASE_URL="sqlite:///${stage2_tmp_dir}/stage2.db"
export PYTHONPYCACHEPREFIX="${stage2_tmp_dir}/pycache"
export DIALOG360_API_KEY=""
export D360_SEND_URL=""
export BOUND_NUMBER="9122-test"

"${stage2_python}" -m unittest discover -v -s tests
"${stage2_python}" -m py_compile \
    app.py storage.py storage_v6_1.py \
    core/conversation.py core/industry.py industries/construction.py
