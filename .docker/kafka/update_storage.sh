#!/bin/sh

CLUSTER_ID=${1}
USERS_AND_PASSWORDS=${2-''}
SASL_MECHANISM=${3}
DEBUG=${4:-false}

# we have to run configure from the original run command so that the /etc/kafka/kafa.properties is available for the kafka-storage command
# <===== START: copy from run script of confluent Dockerfile (https://github.com/confluentinc/kafka-images/blob/master/kafka/include/etc/confluent/docker/run)
. /etc/confluent/docker/bash-config

echo "===> User"
id

echo "===> Configuring ..."
/etc/confluent/docker/configure

# ======> END: copy

kafkaStorageCmd="kafka-storage format --config /etc/kafka/kafka.properties --ignore-formatted --cluster-id $CLUSTER_ID "

export IFS=","
for userAndPassword in $USERS_AND_PASSWORDS; do
  IFS=':' read -r -a data <<< "$userAndPassword"
  kafkaStorageCmd+="--add-scram '$SASL_MECHANISM=[name=${data[0]},password=${data[1]}]' "
done

if $DEBUG ; then
  echo "Executing the following command: $kafkaStorageCmd"
fi
eval "$kafkaStorageCmd"