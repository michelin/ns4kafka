# kafka-namespace-service
Namespaces on top of Kafka Broker, Kafka Connect and Schema Registry

## Key features
- Desired state API
  - Mimics K8S principles
  - Easy to use
    - View : GET /api/namespaces/\<ns-name\>/topics/\<topic-name\>
    - List : GET /api/namespaces/\<ns-name\>/topics/
    - Create : POST /api/namespaces/\<ns-name\>/topics/\<topic-name\>
- Self-service
  - Managed Resources : Topics, Schemas, Connects, KafkaUsers
  - Naming convention
  - Config validation
- Isolation between Kafka Users within a cluster
- Security model
- Disk Quota management
  - Enforce limits per namespace
  - Provide usage metrics
- Cross Namespace ACLs
- Multi cluster




