#!/usr/bin/env bash

release=$1
pset=$2
shift
shift

if [[ -z "$release" || -z "$pset" ]]; then
	echo "usage: autosense.sh release pset args..."
	exit 1
fi

source /cvmfs/cms.cern.ch/cmsset_default.sh 
cd "$release"
eval $(scramv1 runtime -sh)
cd - > /dev/null

python <<EOF
import imp
import json
import shlex
import sys

result = {'outputs': []}
sys.argv = shlex.split("$*")

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
    print(json.dumps(result))
EOF

