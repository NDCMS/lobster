#!/usr/bin/env bash

release=$1
pset=$2
shift
shift

if [[ -z "$release" || -z "$pset" ]]; then
  echo "usage: autosense.sh release pset args..."
  exit 1
fi

# Make sure we don't have any leftover files to confuse things.
if [[ -e out.json ]]; then
  rm -f out.json
fi

source /cvmfs/cms.cern.ch/cmsset_default.sh
cd "$release"
eval $(scramv1 runtime -sh)
cd - > /dev/null

# Decide whether to use python or python3 based on the resolved executable path:
# - if 3rd-to-last element starts with "slc" -> python
# - if it starts with "el"  -> python3
# Preference order:
#   1) Try python (if present); if tag says "slc*" -> use python
#   2) Else try python3 (if present); if tag says "el*" -> use python3
#   3) Else fall back to python if present, else python3, else error

resolve_path() {
  local p="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "$p"
  elif command -v readlink >/dev/null 2>&1; then
    readlink -f "$p" 2>/dev/null || echo "$p"
  else
    echo "$p"
  fi
}

third_to_last_component() {
  local p="$1"
  p="${p%/}"
  p="${p#/}"              # drop leading "/" so array indexing is clean
  local IFS='/'
  read -r -a parts <<< "$p"
  local n="${#parts[@]}"
  if (( n >= 3 )); then
    echo "${parts[n-3]}"
  else
    echo ""
  fi
}

_py=""
_py_path=""
tag=""

# 1) Try python first
_py_path="$(type -P python 2>/dev/null || true)"
if [[ -n "$_py_path" ]]; then
  tag="$(third_to_last_component "$(resolve_path "$_py_path")")"
  if [[ "$tag" == slc* ]]; then
    _py="python"
  fi
fi

# 2) Then try python3
if [[ -z "$_py" ]]; then
  _py_path="$(type -P python3 2>/dev/null || true)"
  if [[ -n "$_py_path" ]]; then
    tag="$(third_to_last_component "$(resolve_path "$_py_path")")"
    if [[ "$tag" == el* ]]; then
      _py="python3"
    fi
  fi
fi

# 3) Final fallback (python first, then python3), else error
if [[ -z "$_py" ]]; then
  if type -P python >/dev/null 2>&1; then
    _py="python"
  elif type -P python3 >/dev/null 2>&1; then
    _py="python3"
  else
    echo "error: neither python nor python3 found in PATH" >&2
    exit 2
  fi
fi

"$_py" <<EOF > /dev/null 2>&1
import imp
import json
import shlex
import sys

result = {'outputs': []}
sys.argv = ["cmsRun","$pset"] + shlex.split("$*")

with open('$pset', 'r') as f:
    source = imp.load_source('cms_config_source', '$pset', f)
    process = source.process
    for label, module in process.outputModules.items():
        result['outputs'].append(module.fileName.value().replace('file:', ''))
    if 'TFileService' in process.services:
        result['outputs'].append(process.services['TFileService'].fileName.value().replace('file:', ''))
        result['merge_command'] = 'hadd'
        result['merge_args'] = ['@outputfiles', '@inputfiles']

    if hasattr(process, 'GlobalTag') and hasattr(process.GlobalTag.globaltag, 'value'):
        result['globaltag'] = process.GlobalTag.globaltag.value()
    with open('out.json','w') as fout:
        json.dump(result,fout)
EOF

cat out.json
# Put an EOL at the end of all this
echo ""

rm -f out.json