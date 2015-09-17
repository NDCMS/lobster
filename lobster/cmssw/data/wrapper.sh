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
	eval $*
	echo
}

echo "[$(date '+%F %T')] wrapper start"
date +%s > t_wrapper_start
echo "=hostname= "$(hostname)
echo "=kernel= "$(uname -a)

print_output "tracing google" traceroute -w 1 www.google.com
print_output "environment at startup" env

# determine locally present stage-out method
LOBSTER_LCG_CP=$(command -v lcg-cp)
LOBSTER_GFAL_COPY=$(command -v gfal-copy)
export LOBSTER_LCG_CP LOBSTER_GFAL_COPY

# determine grid proxy needs
LOBSTER_PROXY_INFO=$(command -v grid-proxy-init)

unset PARROT_HELPER
export PYTHONPATH=python:$PYTHONPATH

if [ -z "$LD_LIBRARY_PATH" ]; then
	export LD_LIBRARY_PATH=lib
else
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib
fi

if [ "x$PARROT_ENABLED" != "x" ]; then
	echo "=parrot= True"
elif [[ ! ( -f "/cvmfs/cms.cern.ch/cmsset_default.sh" \
		&& -n "$LOBSTER_PROXY_INFO" \
		&& ( -n "$LOBSTER_GFAL_COPY" || -n "$LOBSTER_LCG_CP" ) \
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
	# export HTTP_PROXY=${HTTP_PROXY:-http://eddie.crc.nd.edu:3128;DIRECT}
	export HTTP_PROXY=${HTTP_PROXY:-http://eddie.crc.nd.edu:3128}
	export HTTP_PROXY=$(echo $HTTP_PROXY|perl -ple 's/(?<=:\/\/)([^|:;]+)/@ls=split(\/\s\/,`nslookup $1`);$ls[-1]||$1/eg')
	echo ">>> using CVMFS proxy: $HTTP_PROXY"

	# These are allowed to be modified via the environment
	# passed to the job (e.g. via condor)
	export PARROT_DEBUG_FLAGS=${PARROT_DEBUG_FLAGS:-}
	export PARROT_PATH=${PARROT_PATH:-./bin}
	export PARROT_CVMFS_REPO=\
'*:try_local_filesystem
*.cern.ch:pubkey=<BUILTIN-cern.ch.pub>,url=http://cvmfs.fnal.gov:8000/opt/*
*.opensciencegrid.org:pubkey=<BUILTIN-opensciencegrid.org.pub>,url=http://oasis-replica.opensciencegrid.org:8000/cvmfs/*;http://cvmfs.fnal.gov:8000/cvmfs/*;http://cvmfs.racf.bnl.gov:8000/cvmfs/*'

	export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
	export PARROT_CACHE=$TMPDIR
	export PARROT_HELPER=$(readlink -f ${PARROT_PATH%bin*}lib/libparrot_helper.so)

	echo ">>> parrot helper: $PARROT_HELPER"
	print_output "content of $PARROT_CACHE" ls -lt $PARROT_CACHE

	# Variables needed to set symlinks in CVMFS
	# FIXME add heuristic detection?
	# FIXME setting the CMS local site should actually work!
	# export CMS_LOCAL_SITE=${CMS_LOCAL_SITE:-$PWD/siteconfig}
	# echo ">>> CMS local site: $CMS_LOCAL_SITE"

	export OASIS_CERTIFICATES=${OASIS_CERTIFICATES:-/cvmfs/oasis.opensciencegrid.org/mis/certificates}
	echo ">>> OSG certificate location: $OASIS_CERTIFICATES"

	echo ">>> testing parrot usage"
	if [ -n "$(ldd $PARROT_PATH/parrot_run 2>&1 | grep 'not found')" ]; then
		print_output ldd $PARROT_PATH/parrot_run
		exit 169
	else
		echo "parrot OK"
	fi

	# FIXME the -M could be removed once local site setting via
	# environment works
	echo ">>> starting parrot to access CMSSW..."
	exec $PARROT_PATH/parrot_run -M /cvmfs/cms.cern.ch/SITECONF/local=$PWD/siteconfig -t "$PARROT_CACHE/ex_parrot_$(whoami)" bash $0 "$*"
	# exec $PARROT_PATH/parrot_run -t "$PARROT_CACHE/ex_parrot_$(whoami)" bash $0 "$*"
fi

echo ">>> sourcing CMS setup"
source /cvmfs/cms.cern.ch/cmsset_default.sh

if [[ -z "$LOBSTER_PROXY_INFO" || ( -z "$LOBSTER_LCG_CP" && -z "$LOBSTER_GFAL_COPY" ) ]]; then
	echo ">>> sourcing OSG setup"
	slc=$(egrep "Red Hat Enterprise|Scientific|CentOS" /etc/redhat-release | sed 's/.*[rR]elease \([0-9]*\).*/\1/')
	source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.2/current/el$slc-$(uname -m)/setup.sh

	[ -z "$LOBSTER_LCG_CP" ] && export LOBSTER_LCG_CP=$(command -v lcg-cp)
	[ -z "$LOBSTER_GFAL_COPY" ] && export LOBSTER_GFAL_COPY=$(command -v gfal-copy)
fi

print_output "environment after sourcing startup scripts" env
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
scramv1 project -f CMSSW $rel || exit_on_error $? 173 "Failed to create new release"
new_release_top=$(awk -F= '/RELEASETOP/ {print $2}' $rel/.SCRAM/slc*/Environment)
cd $rel
echo ">>> preparing sandbox release $rel"
for i in bin cfipython config lib module python src; do
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

print_output "environment before execution" env

echo "[$(date '+%F %T')] wrapper ready"
date +%s > t_wrapper_ready

print_output "working directory before execution" ls -l

$*
res=$?

print_output "working directory after execution" ls -l

echo "[$(date '+%F %T')] wrapper done"

exit $res
