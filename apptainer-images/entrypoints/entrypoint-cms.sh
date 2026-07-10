#!/bin/sh
set  -e

echo "::: Setting up CMS environment\
 (works only if /cvmfs is mounted on host) ..."
if [ -f /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
  source /cvmfs/cms.cern.ch/cmsset_default.sh
  echo "::: Setting up CMS environment... [done]"
else
  echo "::: Could not set up CMS environment... [ERROR]"
  echo "::: /cvmfs/cms.cern.ch/cmsset_default.sh not found/available"
fi

exec "$@"
