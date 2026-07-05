#!/bin/sh --noprofile

exit_on_error() {
	result=$1
	code=$2
	message=$3

	if [ "$result" != 0 ]; then
		echo "$message"
		exit "$code"
	fi
}

log() {
	if [ $# -gt 2 ]; then
		short=$1
		long=$2
		shift; shift
		echo "==== $long @ $(date) ===="
		eval "$@" | while read line; do
			echo "== $short: $line"
		done
	else
		echo "=== $1 @ $(date)"
	fi
}

date +%s > t_wrapper_start

log "startup" "wrapper started" "echo -e 'hostname: $(hostname)\nkernel: $(uname -a)'"

log "trace" "tracing google" tracepath -m 5 www.google.com
log "env" "environment at startup" env
log "cpu" "cpu info" cat /proc/cpuinfo

# determine locally present stage-out method
LOBSTER_LCG_CP=$(command -v lcg-cp 2>/dev/null)
LOBSTER_GFAL_COPY=$(command -v gfal-copy 2>/dev/null)
export LOBSTER_LCG_CP LOBSTER_GFAL_COPY

# determine grid proxy needs
LOBSTER_PROXY_INFO=$(command -v grid-proxy-init 2>/dev/null)

export PYTHONPATH=python:$PYTHONPATH

if [ -z "$LD_LIBRARY_PATH" ]; then
	export LD_LIBRARY_PATH=lib
else
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib
fi

if [ -d "$PWD/siteconf" ] && [ -z "$CMS_LOCAL_SITE" ]; then
	export CMS_LOCAL_SITE="$PWD/siteconf"
fi

if [ "x$PARROT_ENABLED" != "x" ]; then
	log "using parrot"
elif [ ! \( -f "/cvmfs/cms.cern.ch/cmsset_default.sh" \
		-a -n "$LOBSTER_PROXY_INFO" \
		-a \( -n "$LOBSTER_GFAL_COPY" -o -n "$LOBSTER_LCG_CP" \) \
		-a -f /cvmfs/cms.cern.ch/SITECONF/local/JobConfig/site-local-config.xml \) ]; then
	if [ -f /etc/cvmfs/default.local ]; then
		log "conf" "trying to determine proxy with" cat /etc/cvmfs/default.local

		cvmfsproxy=$(cat /etc/cvmfs/default.local | perl -ne '$file  = ""; while (<>) { s/\\\n//; $file .= $_ }; my $proxy = (grep /PROXY/, split("\n", $file))[0]; $proxy =~ s/^.*="?|"$//g; print $proxy;')
		log "found CVMFS proxy: $cvmfsproxy"
		export HTTP_PROXY=${HTTP_PROXY:-$cvmfsproxy}
	fi

	if [ -n "$OSG_SQUID_LOCATION" ]; then
		export HTTP_PROXY=${HTTP_PROXY:-$OSG_SQUID_LOCATION}
	elif [ -n "$GLIDEIN_Proxy_URL" ]; then
		export HTTP_PROXY=${HTTP_PROXY:-$GLIDEIN_Proxy_URL}
	fi

	# Last safeguard, if everything else fails.  We need a proxy for parrot!
	export FRONTIER_PROXY=${HTTP_PROXY:-$LOBSTER_FRONTIER_PROXY}
	export HTTP_PROXY=${HTTP_PROXY:-$LOBSTER_CVMFS_PROXY}
	export HTTP_PROXY=$(echo "$HTTP_PROXY" | perl -ple 's/(?<=:\/\/)([^|:;]+)/@ls=split(\/\s\/,`nslookup $1`);$ls[-1]||$1/eg')

	log "using CVMFS proxy: $HTTP_PROXY"
	log "using Frontier proxy: $FRONTIER_PROXY"

	frontier=$(echo "$FRONTIER_PROXY" | sed -e 's/[]\/$*.^|[]/\\&/g')
	sed -i -e "s/\$HTTP_PROXY\\>/$frontier/" siteconf/JobConfig/site-local-config.xml

	# These are allowed to be modified via the environment passed to the job (e.g. via condor)
	export PARROT_DEBUG_FLAGS=${PARROT_DEBUG_FLAGS:-}
	export PARROT_PATH=${PARROT_PATH:-./bin}
	export PARROT_CVMFS_REPO='<default-repositories>'

	export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE
	export PARROT_CACHE=${WORKER_TMPDIR:-${TMPDIR:-.}}
	# Make sure that the cvmfs cache is actually shared, this can save up to 1 GB per task in disk usage.
	export PARROT_CVMFS_ALIEN_CACHE=${PARROT_CACHE}/cvmfs

	log "cache" "content of $PARROT_CACHE" ls -lt "$PARROT_CACHE"

	export OASIS_CERTIFICATES=${OASIS_CERTIFICATES:-/cvmfs/oasis.opensciencegrid.org/mis/certificates}
	log "OSG certificate location: $OASIS_CERTIFICATES"

	if [ ! -f /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
		log "testing parrot usage"
		if [ -n "$(ldd "$PARROT_PATH/parrot_run" 2>&1 | grep 'not found')" ]; then
			log "ldd" "linkage of parrot" ldd "$PARROT_PATH/parrot_run"
			exit 169
		else
			log "parrot OK"
		fi

		# FIXME the -M could be removed once local site setting via environment works
		log "starting parrot to access CMSSW..."
		exec "$PARROT_PATH/parrot_run" -M /cvmfs/cms.cern.ch/SITECONF/local="$PWD/siteconf" -M /sbin/ifconfig=/bin/echo -t "$PARROT_CACHE/ex_parrot_$(whoami)" -p "$HTTP_PROXY" bash "$0" "$@"
	fi
fi

log "sourcing CMS setup"
# shellcheck disable=SC1091
. /cvmfs/cms.cern.ch/cmsset_default.sh || exit_on_error $? 175 "Failed to source CMS"

# Determine the OS release version
release=$(grep -Eo 'release [0-9]+' /etc/redhat-release 2>/dev/null | awk '{print $2}')

# Construct the arch variable to handle both slc and el architectures
arch=$(echo sandbox-"${LOBSTER_CMSSW_VERSION}"-*${release}*.tar.bz2 | grep -oE "(slc|el)${release}_[^.]*")

if [ -z "$LOBSTER_PROXY_INFO" ] || { [ -z "$LOBSTER_LCG_CP" ] && [ -z "$LOBSTER_GFAL_COPY" ]; }; then
	log "sourcing OSG setup"
	# shellcheck disable=SC1091
	. /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/"$LOBSTER_OSG_VERSION"/current/el$release-$(uname -m)/setup.sh || exit_on_error $? 175 "Failed to source OSG"

	[ -z "$LOBSTER_LCG_CP" ] && export LOBSTER_LCG_CP=$(command -v lcg-cp 2>/dev/null)
	[ -z "$LOBSTER_GFAL_COPY" ] && export LOBSTER_GFAL_COPY=$(command -v gfal-copy 2>/dev/null)
fi

log "env" "environment after sourcing startup scripts" env
log "proxy" "proxy information" env X509_USER_PROXY=proxy voms-proxy-info
log "dir" "working directory at startup" ls -l

log "creating new release $LOBSTER_CMSSW_VERSION for scram arch $arch"

export SCRAM_ARCH=$arch
scramv1 project -f CMSSW "$LOBSTER_CMSSW_VERSION" || exit_on_error $? 173 "Failed to create new release"

log "testing sandbox-${LOBSTER_CMSSW_VERSION}-${arch}.tar.bz2"
symlinks=$(tar -tvf sandbox-"${LOBSTER_CMSSW_VERSION}"-"${arch}".tar.bz2 2>/dev/null | grep '^l')
if [ -n "$symlinks" ]; then
	log "Found the following symlinks in sandbox-${LOBSTER_CMSSW_VERSION}-${arch}.tar.bz2:"
	log "$symlinks"
else
	log "No symlinks found in sandbox-${LOBSTER_CMSSW_VERSION}-${arch}.tar.bz2"
fi

log "unpacking sandbox-${LOBSTER_CMSSW_VERSION}-${arch}.tar.bz2"
tar xjf sandbox-"${LOBSTER_CMSSW_VERSION}"-"${arch}".tar.bz2 || exit_on_error $? 170 "Failed to unpack sandbox!"

basedir=$PWD
cd "$LOBSTER_CMSSW_VERSION" || exit_on_error $? 171 "Failed to cd into CMSSW release dir"

eval "$(scramv1 runtime -sh)" || exit_on_error $? 174 "The command 'cmsenv' failed!"
cd "$basedir" || exit_on_error $? 171 "Failed to cd back to base dir"

# ---- Choose python on the WORKER, AFTER cmsenv (no python used for detection) ----
resolve_path() {
	p="$1"
	if command -v realpath >/dev/null 2>&1; then
		realpath "$p"
	elif command -v readlink >/dev/null 2>&1; then
		readlink -f "$p" 2>/dev/null || echo "$p"
	else
		echo "$p"
	fi
}

third_to_last_component() {
	# prints 3rd-to-last component of a (resolved) absolute path, e.g.
	# /a/b/c/d/e -> c
	echo "$1" | awk -F/ 'NF>=3 {print $(NF-2)}'
}

choose_python() {
	# Preference order:
	#  1) python if present and its tag is slc*
	#  2) python3 if present and its tag is el*
	#  3) fallback python, else python3, else error

	p=$(command -v python 2>/dev/null)
	if [ -n "$p" ]; then
		tag=$(third_to_last_component "$(resolve_path "$p")")
		case "$tag" in
			slc*) echo python; return 0 ;;
		esac
	fi

	p=$(command -v python3 2>/dev/null)
	if [ -n "$p" ]; then
		tag=$(third_to_last_component "$(resolve_path "$p")")
		case "$tag" in
			el*) echo python3; return 0 ;;
		esac
	fi

	if command -v python >/dev/null 2>&1; then
		echo python
		return 0
	fi
	if command -v python3 >/dev/null 2>&1; then
		echo python3
		return 0
	fi

	echo "error: neither python nor python3 found in PATH" >&2
	return 2
}

PY_CMD=$(choose_python) || exit_on_error $? 176 "Failed to determine python command"
echo "Chosen PY_CMD: $PY_CMD"

log "top" "machine load" top -em b\|head -n 50
log "env" "environment before execution" env
log "wrapper ready"
date +%s > t_wrapper_ready

log "dir" "working directory before execution" ls -l

# Optional diagnostics (won't fail the job if missing)
if command -v python3 >/dev/null 2>&1; then
	echo "python3 --version"
	python3 --version
	echo "which python3"
	command -v python3
fi

if command -v python >/dev/null 2>&1; then
	echo "python --version"
	python --version
	echo "which python"
	command -v python
fi

echo "ls /usr/bin/python*"
ls /usr/bin/python* 2>/dev/null || true

echo ""
echo "==================="

${PY_CMD} -m ensurepip --user
${PY_CMD} -m pip install --user future

orcommand="$*"

# If the incoming command starts with python/python3, rewrite it to the chosen interpreter
case "$orcommand" in
	python\ *|python3\ *)
		rest=${orcommand#* }
		orcommand="$PY_CMD $rest"
		;;
	python|python3)
		orcommand="$PY_CMD"
		;;
esac

echo "Command: $orcommand"

# Execute the (possibly rewritten) command
eval "$orcommand"
res=$?

log "dir" "working directory after execution" ls -l

log "wrapper done"
log "final return status = $res"

exit $res
