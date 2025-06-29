#!/bin/sh

# Inspired from https://github.com/confluentinc/kafka-images/blob/master/kafka/include/etc/confluent/docker/run
echo "Generate kafka.properties"
chmod +x /etc/confluent/docker/bash-config
/etc/confluent/docker/bash-config
/etc/confluent/docker/configure

cat /etc/kafka/kafka.properties

echo "Create admin user"
kafka-storage format --config /etc/kafka/kafka.properties \
  --cluster-id MkU3OEVBNTcwNTJENDM2Qk \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=admin]' \
  --ignore-formatted