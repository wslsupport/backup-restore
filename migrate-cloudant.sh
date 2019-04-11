#!/bin/bash
exit (0)
CUR_DIR=$(cd `dirname $0`; pwd 2>/dev/null)

EXTRACTED_DIRNAME='cloudant-backup'
POD_DIR="/srv"
DEFAULT_NAMESPACE='dsx'
FILES="migrate-cloudant.py cloudant-backup.tar cloudant-backup.log"
MOUNT_DIR="${CUR_DIR}/cloudant-mnt"
PY_SCRIPT="migrate-cloudant.py"
SUB_DIR="cloudant_migration"

function finish() {
    umount ${MOUNT_DIR} 2>/dev/null
    rmdir ${MOUNT_DIR} 2>/dev/null
}
trap finish EXIT

function mountCloudantVol () {
    local pv_name=$(kubectl get pvc -n ${DEFAULT_NAMESPACE} | grep cloudant | uniq | awk '{print $3}')
    local storage_type=$(kubectl get pv ${pv_name} -o jsonpath="{.metadata.annotations.volumeType}")

    mkdir -p ${MOUNT_DIR}
    if [[ "${storage_type}" = "nfs" ]]; then
        local nfs_server=$(kubectl get pv ${pv_name} -o jsonpath="{.spec.nfs.server}")
        local nfs_path=$(kubectl get pv ${pv_name} -o jsonpath="{.spec.nfs.path}")
        mount -t nfs ${nfs_server}:${nfs_path} ${MOUNT_DIR}
    else
        mount -t glusterfs localhost:${pv_name} ${MOUNT_DIR}
    fi
    [[ $? -ne 0 ]] && echo "Failed to mount cloudant volume" && exit 1

    rm -f ${MOUNT_DIR}/testfile && touch ${MOUNT_DIR}/testfile
    [[ $? -ne 0 ]] && echo "Failed to write to cloudant volume" && exit 1
    rm -f ${MOUNT_DIR}/testfile

    return 0
}

function findCloudantPod() {
    cloudant_pod=$(kubectl get po --no-headers -n ${DEFAULT_NAMESPACE} | grep "^cloudant-" | head -n 1 | grep Running | awk '{print $1}')
    namespace="${DEFAULT_NAMESPACE}"

    if [[ -z $cloudant_pod ]]; then
        echo "Unable to find cloudant pod under ${namespace} namespace in Running state"
        echo "Attempting to search for a cloudant pod under all namespaces"

        cloudant_pod=$(kubectl get po --no-headers --all-namespaces | grep " cloudant-" | head -n 1 | grep Running | awk '{print $2}')
        namespace=$(kubectl get po --no-headers --all-namespaces | grep " cloudant-" | head -n 1 | grep Running | awk '{print $1}')

        if [[ -z $cloudant_pod ]] || [[ -z $namespace ]]; then
            echo "Failed to find a running cloudant pod"
            exit 1
        fi
    fi
}

## Start
# Ensure connectivity to kube
kubectl get no &>/dev/null
if [[ $? -ne 0 ]]; then
    echo "Unable to communicate with kube-apiserver with command \"kubectl get no\""
    echo "Please ensure that this node is a master node"
    exit 1
fi

# Ensure python script exists
if [[ ! -f ${CUR_DIR}/${PY_SCRIPT} ]]; then
    echo "Missing required file \"${PY_SCRIPT}\" in directory ${CUR_DIR}"
    exit 1
fi

# Find cloudant PV, mount it
mountCloudantVol

# Find cloudant pod name
findCloudantPod

# Copy python script, log, and archive into cloudant PV
mkdir -p ${MOUNT_DIR}/${SUB_DIR}
for file in ${FILES}; do
    if [[ -f ${CUR_DIR}/${file} ]]; then
        rm -f ${MOUNT_DIR}/${SUB_DIR}/${file}
        cp ${CUR_DIR}/${file} ${MOUNT_DIR}/${SUB_DIR}/
        [[ $? -ne 0 ]] && echo "Failed to copy ${CUR_DIR}/${file} into ${MOUNT_DIR}/${SUB_DIR}/" && exit 1
    fi
done

# delete extracted dir if exists to prevent pre-existing files being restored
rm -rf ${MOUNT_DIR}/${SUB_DIR}/${EXTRACTED_DIRNAME}

# Run the actual migrate python script
echo "Running the following command inside pod ${cloudant_pod} under namespace ${namespace}:"
echo "    \"python ${POD_DIR}/${SUB_DIR}/${PY_SCRIPT} $@\""
echo ""
kubectl exec -n ${namespace} ${cloudant_pod} -- python ${POD_DIR}/${SUB_DIR}/${PY_SCRIPT} "$@"
rc=$?

# Copy python script, log, and archive from cloudant PV
for file in ${FILES}; do
    if [[ -f ${MOUNT_DIR}/${SUB_DIR}/${file}  ]]; then
        rm -f ${CUR_DIR}/${file}
        cp ${MOUNT_DIR}/${SUB_DIR}/${file} ${CUR_DIR}/
        [[ $? -ne 0 ]] && echo "Failed to copy ${CUR_DIR}/${file} into ${MOUNT_DIR}/${SUB_DIR}/" && exit 1
    fi
done

# Exit with rc of python script
exit $rc
