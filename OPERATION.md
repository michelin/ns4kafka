# Operation

The basic structure of a operation is:
``` console
kafkactl OPERATION KIND NAME [flags]
```
with:
* OPERATION the function descibed below.

* KIND the kind of the resource described [here](https://github.com/michelin/ns4kafka/blob/master/RESOURCE.md) 

* NAME the name of the resource deployed.

* flags the specific option.

The list of methods can be accessed with ``kafkactl`` without argument.

Here is a list of the most useful:
- ``apply`` to create a resource
- ``get`` to know the configuration of a deployed resource
- ``api-resources`` to know the supported resource by the api

And here is a list of flags:

- ``--dry-run`` to do the validation of a method without persisting on the cluster Kafka
- ``--recursive`` to recursively search files 


| Operation      | Syntax                                                                                                                                                               | Description                                                      |
| - - - - -      | - - - - -                                                                                                                                                            | - - - - -                                                       |
| apply          | kafkactl apply -f FILENAME [flags]                                                                                                                                   | Apply a configuration change to a resource from a file or stdin |
| api-resources  | kafkactl api-resources                                                                                                                                               | Get resources supported by the API                              |
| delete         | kafkactl delete KIND NAME [flags]                                                                                                                                    | Delete **instantly** a resource on the server                   |
| delete-records | kafkactl delete-records NAME [flags]                                                                                                                                 | Delete records from a topic                                     |
| diff           | kafkactl diff -f FILENAME [flags]                                                                                                                                    | Diff file or stdin against the current configuration            |
| get            | kafkactl get KIND [NAME] [flags]                                                                                                                                     | List resources or get the configuration of one resource         |
| import         | kafkactl import KIND                                                                                                                                                 | Import existing Kafka resources                                                                |
| reset-offsets  | kafkactl reset-offsets --topic TOPIC_ARGS --group GROUP_NAME (--to-earliest \| --to-latest \| --to-datetime DATETIME \| --shift-by NUMBER \| --by-duration DURATION) | Change the offset of a consumer group for a topic               |



## Apply
Option: "--dry-run, --recursive"

Apply is the operation used to create resources on the cluster Kafka.
There is two main methods: the first is by using a yaml descriptor.
````console
kafkactl apply -f namespace.yml
````
The second is by using pipe, this is useful in CI context:
````console
cat namespace.yml | kafkactl apply
````

## Api resources

Print the supported resources of the server, we can use the column "Names" to designate the kind of resource in kafkactl (as in ``kafkactl get kind-name``)
````console
kafkactl api-resources
````
## Delete
Option: "--dry-run"

Delete a resource from the cluster Kafka
````console
kafkactl delete topic prefix_topic-name
````

## Delete records
Option: "--dry-run"

Delete records of a topic
````console
kafkactl delete-records prefix_topic-name
````

## Diff
Option "--recursive"

Compare the resource described by the descriptor with the resource persisted in the cluster kafka
````console
kafkactl diff -f topic.yml
````

## Get
Get is used to get either every resource of a certain kind:
````console
kafkactl get topics
````
or the specification of a ressource:
````console
kafkactl get topic prefix_topic-name
````

## Reset offsets
Option: "--dry-run"

Change the offsets of a consumer group with a specific methods:
- ``--to-earliest`` set offset to its earliest value [reprocess all]
- ``--to-latest``set offset to its latest value [skip all]
- ``--to-datetime`` Set offset to a specific ISO 8601 DateTime with Timezone [yyyy-MM-ddTHH:mm:ss.SSSSXXXXX]
- ``--shift-by`` Shift the offset by a number [negative to reprocess, positive to skip]
- ``--by-duration`` Shift offset by a duration format ISO 8601 [PdDThHmMsS]

for a certain topic:
- ``--topic`` for a specific partition of a topic "topic-name:partition"  or for all partition of a topic "topic-name"
- ``--all-topics`` for all partitions of all topics followed by the consumer group


````console
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name:2 --to-latest
````
````console
kafkactl reset-offsets --group prefix_consumer-group-name --topic prefix_topic-name --to-datetime 2021-03-08T09:30:00.025+02:00
````
````console
kafkactl reset-offsets --group prefix_consumer-group-name --all-topics --by-duration P1DT3H45M30S
````

