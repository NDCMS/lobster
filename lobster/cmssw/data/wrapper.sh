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

print_output() {
	echo
	echo ">>> $1"
	shift
	echo "---8<---"
	eval $*
	echo "--->8---"
	echo
}

echo "[$(date '+%F %T')] wrapper start"
date +%s > t_wrapper_start
echo "=hostname= "$(hostname)
echo "=kernel= "$(uname -a)

print_output "tracing google" traceroute -w 1 www.google.com
print_output "environment at startup" env\|sort

unset PARROT_HELPER
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export PYTHONPATH=/cvmfs/cms.cern.ch/crab/CRAB_2_10_5_patch1/python/:$PYTHONPATH

if [ -z "$LD_LIBRARY_PATH" ]; then
	export LD_LIBRARY_PATH=lib
else
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib
fi

if [ "x$PARROT_ENABLED" != "x" ]; then
	echo "=parrot= True"
	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
else
	if [[ ! ( -f "$VO_CMS_SW_DIR/cmsset_default.sh" \
			&& -f /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh \
			&& -f /cvmfs/cms.cern.ch/SITECONF/local/JobConfig/site-local-config.xml) ]]; then
		if [ -f /etc/cvmfs/default.local ]; then
			print_output "trying to determine proxy with" cat /etc/cvmfs/default.local

			cvmfsproxy=$(cat /etc/cvmfs/default.local|perl -ne '$file  = ""; while (<>) { s/\\\n//; $file .= $_ }; my $proxy = (grep /PROXY/, split("\n", $file))[0]; $proxy =~ s/^.*="?|"$//g; print $proxy;')
			# cvmfsproxy=$(awk -F = '/PROXY/ {print $2}' /etc/cvmfs/default.local|sed 's/"//g')
			echo ">>> found CVMFS proxy: $cvmfsproxy"
			export HTTP_PROXY=${HTTP_PROXY:-$cvmfsproxy}
		fi

		if [ -n "$OSG_SQUID_LOCATION" ]; then
			export HTTP_PROXY=${HTTP_PROXY:-$OSG_SQUID_LOCATION}
		elif [ -n "$GLIDEIN_Proxy_URL" ]; then
			export HTTP_PROXY=${HTTP_PROXY:-$GLIDEIN_Proxy_URL}
		fi

		# Last safeguard, if everything else fails.  We need a
		# proxy for parrot!
		# export HTTP_PROXY=${HTTP_PROXY:-http://ndcms.crc.nd.edu:3128;DIRECT}
		export HTTP_PROXY=${HTTP_PROXY:-http://ndcms.crc.nd.edu:3128}
		# FIXME this hardcodes the IP address of ndcms!
		export HTTP_PROXY=$(echo $HTTP_PROXY|perl -ple 's/(?<=:\/\/)([^|:;]+)/@ls=split(\/\s\/,`nslookup $1`);$ls[-1]||"129.74.85.4"/eg')
		echo ">>> using CVMFS proxy: $HTTP_PROXY"
		export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch

		# These are allowed to be modified via the environment
		# passed to the job (e.g. via condor)
		export PARROT_DEBUG_FLAGS=${PARROT_DEBUG_FLAGS:-}
		export PARROT_PATH=${PARROT_PATH:-./bin}

		export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
		export PARROT_CACHE=$TMPDIR
		export PARROT_HELPER=$(readlink -f ${PARROT_PATH%bin*}lib/libparrot_helper.so)

		echo ">>> parrot helper: $PARROT_HELPER"
		print_output "content of $PARROT_CACHE" ls -lt $PARROT_CACHE

		echo ">>> starting parrot to access CMSSW..."
		exec $PARROT_PATH/parrot_run -m mtab -t "$PARROT_CACHE/ex_parrot_$(whoami)" bash $0 "$*"
	fi

	source $VO_CMS_SW_DIR/cmsset_default.sh
	source /cvmfs/grid.cern.ch/3.2.11-1/etc/profile.d/grid-env.sh
fi

print_output "environment after sourcing startup scripts" env\|sort
print_output "proxy information" env X509_USER_PROXY=proxy voms-proxy-info
print_output "working directory at startup" ls -l

tar xjf sandbox.tar.bz2 || exit_on_error $? 170 "Failed to unpack sandbox!"

basedir=$PWD

rel=$(echo CMSSW_*)
arch=$(ls $rel/.SCRAM/|grep slc) || exit_on_error $? 171 "Failed to determine SL release!"
old_release_top=$(awk -F= '/RELEASETOP/ {print $2}' $rel/.SCRAM/slc*/Environment) || exit_on_error $? 172 "Failed to determine old releasetop!"

export SCRAM_ARCH=$arch

print_output "working directory before release fixing" ls -l

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

print_output "environment before execution" env\|sort

echo "[$(date '+%F %T')] wrapper ready"
date +%s > t_wrapper_ready

print_output "working directory before execution" ls -l

$*
res=$?

print_output "working directory after execution" ls -l

echo "[$(date '+%F %T')] wrapper done"

exit $res
