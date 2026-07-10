#! /bin/bash

trap clean_cvmfsexec EXIT

function clean_cvmfsexec ()
{
    if [[ -z "${NDCMS_ENTRYPOINT_RECURSIVE}"  &&  -n "${CVMFS_TMP_EXEC_DIR}" ]]
    then
        if [[ -e "${CVMFS_TMP_EXEC_DIR}/mnt/afs/crc.nd.edu" ]]
        then
            # safety check, do not remove if afs is still mounted!
            return
        else
            rm -rf "${CVMFS_TMP_EXEC_DIR}"
        fi
    fi
}


if [[ -z "${NDCMS_ENTRYPOINT_RECURSIVE}" ]]
then
    echo "::: Setting up CMS environment\
        (works only if /cvmfs is mounted on host) ..."
fi


SETUP=/cvmfs/cms.cern.ch/cmsset_default.sh
if [ -f "${SETUP}" ]
then
    source "${SETUP}"
    source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.6/current/el7-x86_64/setup.sh
    echo "::: Setting up CMS environment... [done]"

    if [[ -n "${CVMFS_TMP_EXEC_DIR}" ]]
    then
        touch ${CVMFS_TMP_EXEC_DIR}/success
    fi
    exec "$@"
elif [ -n "${NDCMS_ENTRYPOINT_RECURSIVE}" ];
then
    echo "::: /cvmfs/cms.cern.ch/cmsset_default.sh not found/available"
    echo "::: Could not set up CMS environment... [ERROR]"
else
    REPOSITORIES="cms.cern.ch oasis.opensciencegrid.org"
    if [ -n "${CVMFS_REPOSITORIES}" ];
    then
        REPOSITORIES="${REPOSITORIES} $(echo ${CVMFS_REPOSITORIES} | sed 's/,/ /')"
    fi

    export NDCMS_ENTRYPOINT_RECURSIVE=yes
    export CVMFS_TMP_EXEC_DIR=$(mktemp -d -t cvmfsexec-XXXXXX)

    cp /opt/cms/cvmfsexec-mnt ${CVMFS_TMP_EXEC_DIR}
    ${CVMFS_TMP_EXEC_DIR}/cvmfsexec-mnt ${REPOSITORIES} -- "${BASH_SOURCE[0]}" "$@"
    status=$?

    # if exec failed, unset so that the cleanup knows the temp dir is not needed anymore
    unset NDCMS_ENTRYPOINT_RECURSIVE

    # fall back only if cvmfsexec failed to set env in recursive call
    if [[ -f ${CVMFS_TMP_EXEC_DIR}/success ]]
    then
        exit ${status}
    else
        exec "$@"
    fi
fi
