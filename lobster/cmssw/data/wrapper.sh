#!/bin/sh --noprofile

echo "[$(date '+%F %T')] wrapper start"
echo "=hostname= "$(hostname)

if [ "x$PARROT_ENABLED" != "x" ]; then
	echo "=parrot= True"
	export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
else
	which scramv1 > /dev/null 2>&1

	if [ $? != 0 ]; then
		export CMS_LOCAL_SITE=T3_US_NotreDame
		export HTTP_PROXY="http://ndcms.crc.nd.edu:3128"
		export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
		export PARROT_HELPER=/afs/nd.edu/user37/ccl/software/cctools/bin/parrot_helper.so
		export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
		echo ">>> fixing JobConfig..."
		sconf=/cvmfs/cms.cern.ch/SITECONF/local/JobConfig/
		sname=site-local-config.xml
		/afs/nd.edu/user37/ccl/software/cctools/bin/parrot_run -t "$TMPDIR/ex_parrot" /bin/cp $sconf$sname $sname
		sed -i -e "s@//pscratch/osg/app/cmssoft/cms/@/cvmfs/cms.cern.ch/@" $sname
		echo "$sconf$sname	$sname" > mtab
		echo ">>> starting parrot to access CMSSW..."
		exec /afs/nd.edu/user37/ccl/software/cctools/bin/parrot_run -m mtab -t "$TMPDIR/ex_parrot" $0 "$*"
	fi
fi

tar xjf sandbox.tar.bz2 || exit 123

basedir=$PWD

rel=$(echo CMSSW_*)
arch=$(ls $rel/.SCRAM/|grep slc)
old_release_top=$(awk -F= '/RELEASETOP/ {print $2}' $rel/.SCRAM/slc*/Environment)

export SCRAM_ARCH=$arch

echo ">>> creating new release $rel"
mkdir tmp && cd tmp
scramv1 project CMSSW $rel
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

eval $(scramv1 runtime -sh)
echo $?
cd "$basedir"

echo
echo "---8<---"
env
echo "--->8---"
echo

echo
echo "[$(date '+%F %T')] wrapper ready"
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
