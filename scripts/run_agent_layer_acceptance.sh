#!/usr/bin/env bash
set -euo pipefail

agent_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hubflo-agent-layer.XXXXXX")"
trap 'rm -rf "${agent_tmp_dir}"' EXIT

agent_python="${HUBFLO_TEST_PYTHON:-python3}"
export DATABASE_URL="sqlite:///${agent_tmp_dir}/agent-layer.db"
export PYTHONPYCACHEPREFIX="${agent_tmp_dir}/pycache"
export DIALOG360_API_KEY=""
export D360_SEND_URL=""
export BOUND_NUMBER="9122-test"

"${agent_python}" -m unittest -v tests.test_agent_layer_2
"${agent_python}" -m py_compile \
    agent_layer/__init__.py agent_layer/contracts.py agent_layer/models.py \
    agent_layer/security.py agent_layer/providers.py agent_layer/persistence.py \
    agent_layer/runtime.py agent_layer/stage2.py tests/test_agent_layer_2.py
