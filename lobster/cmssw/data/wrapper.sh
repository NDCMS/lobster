#!/bin/sh

echo "---<[$(date '+%F %T') wrapper start]>---"

if [ "x$PARROT_ENABLED" != "x" ]; then
	echo "Opperating with parrot enabled"
	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
else
	which scramv1 > /dev/null 2>&1

	if [ $? != 0 ]; then
		export HTTP_PROXY="http://ndcms.crc.nd.edu:3128"
		export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
		export PARROT_HELPER=/afs/nd.edu/user37/ccl/software/cctools/bin/parrot_helper.so
		export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
		exec /afs/nd.edu/user37/ccl/software/cctools/bin/parrot_run $0 "$*"
	fi
fi

tar xjf sandbox.tar.bz2 || exit 123

cd CMSSW_*
eval $(scramv1 runtime -sh)
cd -

echo "---<[$(date '+%F %T') wrapper ready]>---"
$*
echo "---<[$(date '+%F %T') wrapper done]>---"
