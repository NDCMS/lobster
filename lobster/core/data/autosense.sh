#!/usr/bin/env bash

releas=$1
pset=$2
shift
shift

if [[ $# -ge 1 || -n "$release" || -n "$pset" ]]; then
	echo "usage: autosense.sh release pset args..."
	exit 1
fi

source /cvmfs/cms.cern.ch/cmsset_default.sh 
workdir=$(mktemp -d)
cd "$workdir"
cp $* .

slc=$(egrep "Red Hat Enterprise|Scientific|CentOS" /etc/redhat-release | sed 's/.*[rR]elease \([0-9]*\).*/\1/')
arch=$(echo sandbox-${version}-slc${slc}*.tar.bz2 |grep -oe "slc${slc}_[^.]*")

export SCRAM_ARCH=$arch
scramv1 project -f CMSSW $version || exit 1
tar xjf sandbox-${version}-${arch}.tar.bz2 || exit 1

cd $version
eval $(scramv1 runtime -sh)

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

rm -rf "$workdir"
