#!/bin/sh --noprofile

exit_on_error() {
	result=$1
	code=$2
	message=$3

	if [ $1 != 0 ]; then
		echo $3
		exit $2
	fi
}

echo "[$(date '+%F %T')] wrapper start"
date +%s > t_wrapper_start
echo "=hostname= "$(hostname)

export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export PYTHONPATH=/cvmfs/cms.cern.ch/crab/CRAB_2_10_5_patch1/python/:$PYTHONPATH

if [ "x$PARROT_ENABLED" != "x" ]; then
	echo "=parrot= True"
	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
else
	if [[ ! ( -f "$VO_CMS_SW_DIR/cmsset_default.sh" && -f /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh ) ]]; then
		# export MYCACHE=$PWD
		export MYCACHE=$TMPDIR
		# export PARROT_DEBUG_FLAGS="-d cvmfs"
		export PARROT_DEBUG_FLAGS=
		export CMS_LOCAL_SITE=T3_US_NotreDame
		export HTTP_PROXY="http://ndcms.crc.nd.edu:3128"
		export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
		export PARROT_HELPER=/afs/nd.edu/user37/ccl/software/cctools-lobster/bin/parrot_helper.so
		export PARROT_EXEC=/afs/nd.edu/user37/ccl/software/cctools-lobster/bin/parrot_run
		export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch

		echo ">>> parrot helper: $PARROT_HELPER"
		echo ">>> content of $MYCACHE:"
		ls -lt $MYCACHE

		echo ">>> fixing JobConfig..."
		sconf=/cvmfs/cms.cern.ch/SITECONF/local/JobConfig/
		sname=site-local-config.xml
		$PARROT_EXEC -t "$MYCACHE/ex_parrot_$(whoami)" /bin/cp $sconf$sname $sname
		exit_on_error $? 200 "Failed to fix site configuration!"
		sed -i -e "s@//pscratch/osg/app/cmssoft/cms/@/cvmfs/cms.cern.ch/@" $sname
		echo "$sconf$sname	$sname" > mtab
		echo ">>> starting parrot to access CMSSW..."
		exec $PARROT_EXEC $PARROT_DEBUG_FLAGS -m mtab -t "$MYCACHE/ex_parrot_$(whoami)" bash $0 "$*"
	fi

	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
fi

echo
echo ">>> proxy information"
echo "---8<---"
env X509_USER_PROXY=proxy voms-proxy-info
echo "--->8---"
echo
echo ">>> working directory at startup"
echo "---8<---"
ls -l
echo "--->8---"
echo

tar xjf sandbox.tar.bz2 || exit_on_error $? 170 "Failed to unpack sandbox!"

basedir=$PWD

rel=$(echo CMSSW_*)
arch=$(ls $rel/.SCRAM/|grep slc) || exit_on_error $? 171 "Failed to determine SL release!"
old_release_top=$(awk -F= '/RELEASETOP/ {print $2}' $rel/.SCRAM/slc*/Environment) || exit_on_error $? 172 "Failed to determine old releasetop!"

export SCRAM_ARCH=$arch

echo
echo ">>> working directory before release fixing"
echo "---8<---"
ls -l
echo "--->8---"
echo
echo ">>> creating new release $rel"
mkdir tmp || exit_on_error $? 173 "Failed to create temporary directory"
cd tmp
scramv1 project CMSSW $rel || exit_on_error $? 173 "Failed to create new release"
new_release_top=$(awk -F= '/RELEASETOP/ {print $2}' $rel/.SCRAM/slc*/Environment)
cd $rel
echo ">>> preparing sandbox release $rel"
for i in bin lib module python src; do
	rm -rf "$i"
	mv "$basedir/$rel/$i" .
	# ls -lR $i
done


echo ">>> fixing python paths"
for f in $(find -iname __init__.py); do
	sed -i -e "s@$old_release_top@$new_release_top@" "$f"
done

eval $(scramv1 runtime -sh) || exit_on_error $? 174 "The command 'cmsenv' failed!"
cd "$basedir"

echo
echo "---8<---"
env
echo "--->8---"
echo

echo
echo "[$(date '+%F %T')] wrapper ready"
date +%s > t_wrapper_ready
echo
echo ">>> working directory before execution"
echo "---8<---"
ls -l
echo "--->8---"
echo

$*
res=$?

echo
echo ">>> working directory after execution"
echo "---8<---"
ls -l
echo "--->8---"
echo
echo "[$(date '+%F %T')] wrapper done"

exit $res
