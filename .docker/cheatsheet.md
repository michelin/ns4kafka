# Cheat Sheet

This is a cheatsheet for Kafka. Connect to the Kafka container using the following command:

```bash
docker exec -it broker sh
```

## Table of Contents

* [ACLs](#acls)
    * [List](#list)

## ACLs

### List

```bash
kafka-acls --bootstrap-server localhost:9092 --command-config client.properties --list
```