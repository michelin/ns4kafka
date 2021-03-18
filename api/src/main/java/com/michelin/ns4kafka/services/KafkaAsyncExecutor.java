package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import io.micronaut.context.annotation.EachBean;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class KafkaAsyncExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);
    private Admin adminClient;
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    TopicRepository topicRepository;
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;


    public KafkaAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) throws MalformedURLException {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    //TODO abstract synchronization process to handle different Kafka "models"
    // ie : cloud API vs AdminClient
    public void run(){

        // execute topic changes
        if(this.kafkaAsyncExecutorConfig.isManageTopics()) {
            synchronizeTopics();
        }

        // execute ACL changes
        if(this.kafkaAsyncExecutorConfig.isManageAcls()) {
            synchronizeACLs();
        }

        if(this.kafkaAsyncExecutorConfig.isManageUsers()) {
            // TODO User + Password requires AdminClient and Brokers >= 2.7.0
            //  https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API
            //  Until then create the user/password without ns4kafka
            throw new UnsupportedOperationException("Not implemented, contributions welcome");
        }

    }
    /**** TOPICS MANAGEMENT ***/
    public void synchronizeTopics(){
        LOG.debug("Starting topic collection for cluster "+kafkaAsyncExecutorConfig.getName());
        try {
            // List topics from broker
            Map<String, Topic> brokerTopicList = collectBrokerTopicList();
            // List topics from ns4kafka Repository
            List<Topic> ns4kafkaTopicList = topicRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

            // Compute toCreate, toDelete, and toUpdate lists
            List<Topic> toCreate = ns4kafkaTopicList.stream()
                    .filter(topic -> !brokerTopicList.containsKey(topic.getMetadata().getName()))
                    .collect(Collectors.toList());

            List<Topic> toDelete = brokerTopicList.values()
                    .stream()
                    .filter(topic -> ns4kafkaTopicList.stream().noneMatch(topic1 -> topic1.getMetadata().getName().equals(topic.getMetadata().getName())))
                    .collect(Collectors.toList());

            List<Topic> toCheckConf = ns4kafkaTopicList.stream()
                    .filter(topic -> brokerTopicList.containsKey(topic.getMetadata().getName()))
                    .collect(Collectors.toList());
            Map<ConfigResource, Collection<AlterConfigOp>> toUpdate = toCheckConf.stream()
                    .map(topic -> {
                        Map<String,String> actualConf = brokerTopicList.get(topic.getMetadata().getName()).getSpec().getConfigs();
                        Map<String,String> expectedConf = topic.getSpec().getConfigs() == null ? Map.of() : topic.getSpec().getConfigs();
                        Collection<AlterConfigOp> topicConfigChanges = computeConfigChanges(expectedConf,actualConf);
                        if(topicConfigChanges.size()>0){
                            ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getMetadata().getName());
                            return Map.entry(cr,topicConfigChanges);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull) //TODO can we avoid this filter ?
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if(LOG.isDebugEnabled()){
                LOG.debug("Topics to create : "+ toCreate.stream().map(t -> t.getMetadata().getName()).collect(Collectors.joining(", ")));
                //TODO reenable
                // LOG.debug("Topics to delete : "+String.join(", ", toDelete.stream().map(t -> t.getMetadata().getName()).collect(Collectors.toList())));
                LOG.debug("Topics to delete : "+toDelete.size());
                LOG.debug("Topic configs to update : "+toUpdate.size());
                for (Map.Entry<ConfigResource,Collection<AlterConfigOp>> e : toUpdate.entrySet()) {
                    for (AlterConfigOp op : e.getValue()) {
                        LOG.debug(e.getKey().name()+" "+op.opType().toString()+" " +op.configEntry().name()+"("+op.configEntry().value()+")");
                    }
                }
            }
            //creating topics
            createTopics(toCreate);
            //delete
            deleteTopics(toDelete);
            //alter
            alterTopics(toUpdate, toCheckConf);

        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            LOG.error("Error", e);
        } catch (InterruptedException e) {
            LOG.error("Error", e);
            Thread.currentThread().interrupt();
        }

    }
    private void deleteTopics(List<Topic> topics) {
        //TODO What's the best way to prevent delete __consumer_offsets and other internal topics ?
        // delete only topics that belongs to a namespace and ignore others ?
        // create a technical namespace with internal topics ? risky
        // delete synchronously from DELETE API calls ?
        // other ?
        /*
        DeleteTopicsResult deleteTopicsResult = getAdminClient().deleteTopics(topics.stream().map(topic -> topic.getMetadata().getName()).collect(Collectors.toList()));
        deleteTopicsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        .whenComplete((unused, throwable) ->{
                            Topic deleted = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey())).findFirst().get();
                            if(throwable!=null){
                                LOG.error(String.format("Error while deleting topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                LOG.info(String.format("Success deleting topic %s on %s : [%s]", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName()));
                            }
                        })
                );
         */
    }
    public void deleteTopic(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        getAdminClient().deleteTopics(List.of(topic.getMetadata().getName())).all().get(30, TimeUnit.SECONDS);
    }
    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult = getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        //TODO blocking
                        // we're already on a dedicated calling thread, by threading again we risk calling this while another call is in progress
                        // @Scheduled from micronaut guarantees with fixedDelay a delay between the end of previous call and beginning of next call
                        .whenComplete((unused, throwable) ->{
                            Topic updatedTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey().name())).findFirst().get();
                            if(throwable!=null){
                                updatedTopic.setStatus(Topic.TopicStatus.ofFailed("Error while updating topic configs: "+throwable.getMessage()));
                                LOG.error(String.format("Error while updating topic configs %s on %s", mapEntry.getKey().name(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                Collection<AlterConfigOp> ops = toUpdate.get(mapEntry.getKey());
                                updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                updatedTopic.getMetadata().setGeneration(updatedTopic.getMetadata().getGeneration()+1);
                                updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));
                                LOG.info(String.format("Success updating topic configs %s on %s : [%s]",
                                        mapEntry.getKey().name(),
                                        this.kafkaAsyncExecutorConfig.getName(),
                                        ops.stream().map(alterConfigOp -> alterConfigOp.toString()).collect(Collectors.joining(","))));
                            }
                            topicRepository.create(updatedTopic);
                        })
                );
    }
    private void createTopics(List<Topic> topics) {
        List<NewTopic> newTopics = topics.stream()
                .map(topic -> {
                    LOG.debug(String.format("Creating topic %s on %s",topic.getMetadata().getName(),topic.getMetadata().getCluster()));
                    NewTopic newTopic = new NewTopic(topic.getMetadata().getName(),topic.getSpec().getPartitions(), (short) topic.getSpec().getReplicationFactor());
                    newTopic.configs(topic.getSpec().getConfigs());
                    LOG.debug(newTopic.toString());
                    return newTopic;
                })
                .collect(Collectors.toList());
        CreateTopicsResult createTopicsResult = getAdminClient().createTopics(newTopics);
        createTopicsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        //TODO blocking
                        .whenComplete((unused, throwable) ->{
                            Topic createdTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey())).findFirst().get();
                            if(throwable!=null){
                                createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: "+throwable.getMessage()));
                                LOG.error(String.format("Error while creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                createdTopic.getMetadata().setGeneration(1);
                                createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                                LOG.info(String.format("Success creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()));
                            }
                            topicRepository.create(createdTopic);
                        })
                );
    }
    private Map<String, Topic> collectBrokerTopicList() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = getAdminClient().listTopics().listings()
                .get(30, TimeUnit.SECONDS)
                .stream()
                .map(topicListing -> topicListing.name())
                .collect(Collectors.toList());
        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topicNames).all().get();
        // Create a Map<TopicName, Map<ConfigName, ConfigValue>> for all topics
        // includes only Dynamic config properties
        return getAdminClient()
                .describeConfigs(topicNames.stream()
                        .map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s))
                        .collect(Collectors.toList())
                )
                //.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC,"*")))
                .all()
                .get(30, TimeUnit.SECONDS)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        configResourceConfigEntry -> configResourceConfigEntry.getKey().name()
                        , configResourceConfigEntry ->
                                configResourceConfigEntry.getValue().entries()
                                        .stream()
                                        .filter( configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
                ))
                .entrySet()
                .stream()
                .map(stringMapEntry -> Topic.builder()
                        .metadata(ObjectMeta.builder()
                                .cluster(kafkaAsyncExecutorConfig.getName())
                                .name(stringMapEntry.getKey())
                                .build())
                        .spec(Topic.TopicSpec.builder()
                                .replicationFactor(topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size())
                                .partitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size())
                                .configs(stringMapEntry.getValue())
                                .build())
                        .build()
                )
                .collect(Collectors.toMap( topic -> topic.getMetadata().getName(), Function.identity()));
    }
    private Collection<AlterConfigOp> computeConfigChanges(Map<String,String> expected, Map<String,String> actual){
        List<AlterConfigOp> toCreate = expected.entrySet()
                .stream()
                .filter(expectedEntry -> !actual.containsKey(expectedEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());
        List<AlterConfigOp> toDelete = actual.entrySet()
                .stream()
                .filter(actualEntry -> !expected.containsKey(actualEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),null), AlterConfigOp.OpType.DELETE))
                .collect(Collectors.toList());
        List<AlterConfigOp> toChange = expected.entrySet()
                .stream()
                .filter(expectedEntry -> {
                    if (actual.containsKey(expectedEntry.getKey())) {
                        String actualVal = actual.get(expectedEntry.getKey());
                        String expectedVal = expectedEntry.getValue();
                        return !expectedVal.equals(actualVal);
                    }
                    return false;
                })
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());

        List<AlterConfigOp> total = new ArrayList<>();
        total.addAll(toCreate);
        total.addAll(toDelete);
        total.addAll(toChange);

        return total;
    }

    /**** ACLS MANAGEMENT ***/
    public void synchronizeACLs(){

        LOG.debug("Starting ACL collection for cluster "+kafkaAsyncExecutorConfig.getName());
        try {
            // List ACLs from broker
            List<AclBinding> brokerACLs = collectBrokerACLs(true);
            List<AclBinding> ns4kafkaACLs = collectNs4KafkaACLs();


            List<AclBinding> toCreate = ns4kafkaACLs.stream()
                    .filter(aclBinding -> !brokerACLs.contains(aclBinding))
                    .collect(Collectors.toList());
            List<AclBinding> toDelete = brokerACLs.stream()
                    .filter(aclBinding -> !ns4kafkaACLs.contains(aclBinding))
                    .collect(Collectors.toList());

            if(LOG.isDebugEnabled()){
                brokerACLs.stream()
                        .filter(aclBinding -> ns4kafkaACLs.contains(aclBinding))
                        .forEach(aclBinding -> LOG.debug("Found in both : "+aclBinding.toString()));
                toCreate.forEach(aclBinding -> LOG.debug("to create : "+aclBinding.toString()));
                toDelete.forEach(aclBinding -> LOG.debug("to delete : "+aclBinding.toString()));
            }


            // Execute toAdd list BEFORE toDelete list to avoid breaking ACL on connected user
            // such as deleting <LITERAL "toto.titi"> only to add one second later <PREFIX "toto.">
            createACLs(toCreate);
            deleteACLs(toDelete);


        }catch (KafkaStoreException | ExecutionException | TimeoutException e){
            LOG.error("Error", e);
        } catch (InterruptedException e) {
            LOG.error("Error", e);
            Thread.currentThread().interrupt();
        }

    }

    private void deleteACLs(List<AclBinding> toDelete) {
        getAdminClient()
                .deleteAcls(toDelete.stream()
                        .map(aclBinding -> aclBinding.toFilter())
                        .collect(Collectors.toList()))
                .values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        //TODO blocking
                        .whenComplete((unused, throwable) -> {
                            if(throwable!=null){
                                LOG.error(String.format("Error while deleting ACL %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                LOG.info(String.format("Success deleting ACL %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()));
                            }

                        })
                );
    }

    private void createACLs(List<AclBinding> toCreate) {
        getAdminClient().createAcls(toCreate).values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        //TODO blocking
                        .whenComplete((unused, throwable) -> {
                            if(throwable!=null){
                                LOG.error(String.format("Error while creating ACL %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                LOG.info(String.format("Success creating ACL %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()));
                            }

                    })
                );
    }

    private List<AclBinding> collectNs4KafkaACLs(){
        // List ACLs from ns4kafka Repository and apply the following rules
        // Whenever the Permission is OWNER, create 2 entries (one READ and one WRITE)
        // This is necessary to translate ns4kafka grouped AccessControlEntry (OWNER, WRITE, READ)
        // into Kafka Atomic ACL (READ and WRITE)

        // TODO this returns only the default user with ACL "inherited" from the namespace
        //   at some point we want to manage multiple users within a namespace, each having their own ACLs.

        List<AclBinding> ns4kafkaACLs = namespaceRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .flatMap(namespace -> accessControlEntryRepository.findAllGrantedToNamespace(namespace.getName())
                        .stream()
                        .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC ||
                                accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.GROUP)
                        //1-N ACE to List<AclBinding>
                        .flatMap(accessControlEntry ->
                                buildAclBindingsFromAccessControlEntry(accessControlEntry, namespace.getDefaulKafkatUser()).stream()))
                .collect(Collectors.toList());

        if(LOG.isDebugEnabled()) {
            LOG.debug("ACLs found on ns4kafka : "+ns4kafkaACLs.size());
            ns4kafkaACLs.forEach(aclBinding -> LOG.debug(aclBinding.toString()));
        }
        return ns4kafkaACLs;
    }
    private List<AclBinding> collectBrokerACLs(boolean managedUsersOnly) throws ExecutionException, InterruptedException, TimeoutException {
        //TODO soon : manage IDEMPOTENT_WRITE on CLUSTER 'kafka-cluster'
        //TODO eventually : manage DELEGATION_TOKEN and TRANSACTIONAL_ID
        //TODO eventually : manage host ?
        //TODO never ever : manage CREATE and DELETE Topics (managed by ns4kafka !)

        List<ResourceType> validResourceTypes = List.of(ResourceType.TOPIC, ResourceType.GROUP);

        // keep only TOPIC and GROUP Resource Types
        // TODO alert when records are filtered out ?
        List<AclBinding> userACLs = getAdminClient()
                .describeAcls(AclBindingFilter.ANY)
                .values().get(10, TimeUnit.SECONDS)
                .stream()
                .filter(aclBinding -> validResourceTypes.contains(aclBinding.pattern().resourceType()))
                .collect(Collectors.toList());

        LOG.debug("ACLs found on Broker (total) : "+userACLs.size());
        if(LOG.isTraceEnabled()) {

            userACLs.forEach(aclBinding -> {
                LOG.trace(aclBinding.toString());
            });
        }
        //TODO add parameter to cluster configuration to scope ALL users vs "namespace" managed users
        // as of now, this will prevent deletion of ACLs for users not in ns4kafka scope
        if(managedUsersOnly) {
            // we first collect the list of Users managed in ns4kafka
            List<String> managedUsers = namespaceRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                    .stream()
                    //TODO managed user list should include not only "defaultKafkaUser" (MVP35)
                    //1-N Namespace to KafkaUser
                    .flatMap(namespace -> List.of("User:"+namespace.getDefaulKafkatUser()).stream())
                    .collect(Collectors.toList());
            // And then filter out the AclBinding to retain only those matching.
            userACLs = userACLs.stream()
                    .filter(aclBinding -> managedUsers.contains(aclBinding.entry().principal()))
                    .collect(Collectors.toList());
            LOG.debug("ACLs found on Broker (managed scope) : "+userACLs.size());
        }


        if(LOG.isDebugEnabled()) {

            userACLs.forEach(aclBinding -> {
                LOG.debug(aclBinding.toString());
            });
        }


        return userACLs;
    }

    private List<AclBinding> buildAclBindingsFromAccessControlEntry(AccessControlEntry accessControlEntry, String kafkaUser){
        //convert pattern, convert resource type from NS4Kafka to org.apache.kafka.common types
        PatternType patternType = PatternType.fromString(accessControlEntry.getSpec().getResourcePatternType().toString());
        ResourceType resourceType = ResourceType.fromString(accessControlEntry.getSpec().getResourceType().toString());
        ResourcePattern resourcePattern = new ResourcePattern(resourceType,
                accessControlEntry.getSpec().getResource(),
                patternType);

        //generate the required AclOperation based on ResourceType
        List<AclOperation> targetAclOperations=null;
        if(accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER){
            targetAclOperations = computeAclOperationForOwner(resourceType);
        }else {
            //should be READ or WRITE
            targetAclOperations = List.of(AclOperation.fromString(accessControlEntry.getSpec().getPermission().toString()));
        }
        return targetAclOperations.stream().map(aclOperation ->
                new AclBinding(resourcePattern, new org.apache.kafka.common.acl.AccessControlEntry("User:"+kafkaUser, "*", aclOperation, AclPermissionType.ALLOW)))
                .collect(Collectors.toList());
    }

    private List<AclOperation> computeAclOperationForOwner(ResourceType resourceType) {
        switch (resourceType){
            case TOPIC:
                return List.of(AclOperation.WRITE,AclOperation.READ);
            case GROUP:
                return List.of(AclOperation.READ);
            case CLUSTER:
            case TRANSACTIONAL_ID:
            case DELEGATION_TOKEN:
            default:
                throw new IllegalArgumentException("Not implemented yet :"+resourceType.toString());
        }
    }

    public Map<Namespace,List<AccessControlEntry>> generateNamespaces() throws InterruptedException, ExecutionException, TimeoutException {
        List<AclOperation> validAclOperations = List.of(AclOperation.WRITE, AclOperation.READ);
        List<AclBinding> userACLs = collectBrokerACLs(false)
                .stream()
                .filter(aclBinding -> validAclOperations.contains(aclBinding.entry().operation()))
                .collect(Collectors.toList());

        List<String> users = userACLs.stream()
                .map(aclBinding -> aclBinding.entry().principal())
                .distinct()
                .collect(Collectors.toList());

        List<Namespace> namespaces = users.stream().map(s ->
            Namespace.builder()
                    .cluster(this.kafkaAsyncExecutorConfig.getName())
                    .defaulKafkatUser(s.replace("User:",""))
                    .diskQuota(0)
                    .build()
        ).collect(Collectors.toList());

        Map<Namespace, List<AccessControlEntry>> ret = users
                .stream()
                .map(s -> Map.entry(Namespace.builder()
                                .name("namespace_"+s.replace("User:",""))
                                .cluster(this.kafkaAsyncExecutorConfig.getName())
                                .defaulKafkatUser(s.replace("User:",""))
                                .diskQuota(0)
                                .build(),
                        userACLs.stream()
                                .filter(aclBinding -> aclBinding.entry().principal().equals(s))
                        .map(aclBinding -> AccessControlEntry.builder()
                                .metadata(ObjectMeta.builder()
                                        .cluster(this.kafkaAsyncExecutorConfig.getName())
                                        .namespace("namespace_"+s.replace("User:",""))
                                        .labels(Map.of("grantedBy",AccessControlEntry.ADMIN))
                                        .build())
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .grantedTo("namespace_"+s.replace("User:",""))
                                        .permission(AccessControlEntry.Permission.valueOf(aclBinding.entry().operation().toString()))
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.valueOf(aclBinding.pattern().patternType().toString()))
                                        .resourceType(AccessControlEntry.ResourceType.valueOf(aclBinding.pattern().resourceType().toString()))
                                        .resource(aclBinding.pattern().name())
                                        .build())
                                .build())
                        .collect(Collectors.toList())
                ))
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

                            /*
                            .acls(userACLs.stream()
                                    .filter(aclBinding -> aclBinding.entry().principal().equals(s))
                                    .map(aclBinding -> AccessControlEntry.builder()
                                            .metadata(ObjectMeta.builder()
                                                    .
                                            .build())
                                    .permission(AccessControlEntry.Permission.valueOf(aclBinding.entry().operation().toString()))
                                    .resourcePatternType(AccessControlEntry.ResourcePatternType.valueOf(aclBinding.pattern().patternType().toString()))
                                    .resourceType(AccessControlEntry.ResourceType.valueOf(aclBinding.pattern().resourceType().toString()))
                                    .resource(aclBinding.pattern().name())
                                    .build())
                                    .collect(Collectors.toList()))
                                        .build()
                    */

        ret.entrySet().forEach(entry -> {
                        // recover TOPIC Pattern Policies
                        List<String> prefixes = entry.getValue().stream()
                                .filter(accessControlEntry ->
                                        accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC &&
                                                accessControlEntry.getSpec().getResourcePatternType()== AccessControlEntry.ResourcePatternType.PREFIXED)
                                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                                .collect(Collectors.toList());
                        // If READ+WRITE, guess OWNER
                        prefixes.forEach(s -> {
                            if(entry.getValue().stream().anyMatch(accessControlEntry -> accessControlEntry.getSpec().getResource().equals(s)
                                    && accessControlEntry.getSpec().getResourceType()== AccessControlEntry.ResourceType.TOPIC
                                    && accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED
                                    && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.READ)
                                    && entry.getValue().stream().anyMatch(accessControlEntry -> accessControlEntry.getSpec().getResource().equals(s)
                                    && accessControlEntry.getSpec().getResourceType()== AccessControlEntry.ResourceType.TOPIC
                                    && accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED
                                    && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.WRITE)
                            ){
                                entry.setValue(entry.getValue().stream().filter(accessControlEntry -> !accessControlEntry.getSpec().getResource().equals(s)).collect(Collectors.toList()));
                                entry.getValue().add(AccessControlEntry.builder()
                                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                                .grantedTo(entry.getKey().getName())
                                                .resource(s)
                                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                                .permission(AccessControlEntry.Permission.OWNER)
                                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                                .build())
                                        .metadata(ObjectMeta.builder()
                                                .cluster(this.kafkaAsyncExecutorConfig.getName())
                                                .namespace(entry.getKey().getName())
                                                .labels(Map.of("grantedBy",AccessControlEntry.ADMIN))
                                                .build())
                                        .build()
                                );

                            }
                        });
            // recover TOPIC LITERAL Policies
            prefixes = entry.getValue().stream()
                    .filter(accessControlEntry ->
                            accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC &&
                                    accessControlEntry.getSpec().getResourcePatternType()== AccessControlEntry.ResourcePatternType.LITERAL)
                    .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                    .collect(Collectors.toList());
            // If READ+WRITE, guess OWNER
            prefixes.forEach(s -> {
                if(entry.getValue().stream().anyMatch(accessControlEntry -> accessControlEntry.getSpec().getResource().equals(s)
                        && accessControlEntry.getSpec().getResourceType()== AccessControlEntry.ResourceType.TOPIC
                        && accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL
                        && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.READ)
                        && entry.getValue().stream().anyMatch(accessControlEntry -> accessControlEntry.getSpec().getResource().equals(s)
                        && accessControlEntry.getSpec().getResourceType()== AccessControlEntry.ResourceType.TOPIC
                        && accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL
                        && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.WRITE)
                ){
                    entry.setValue(entry.getValue().stream().filter(accessControlEntry -> !accessControlEntry.getSpec().getResource().equals(s)).collect(Collectors.toList()));
                    entry.getValue().add(AccessControlEntry.builder()
                            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                    .grantedTo(entry.getKey().getName())
                                    .resource(s)
                                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                    .permission(AccessControlEntry.Permission.OWNER)
                                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                    .build())
                            .metadata(ObjectMeta.builder()
                                    .cluster(this.kafkaAsyncExecutorConfig.getName())
                                    .namespace(entry.getKey().getName())
                                    .labels(Map.of("grantedBy",AccessControlEntry.ADMIN))
                                    .build())
                            .build()
                    );

                }
            });
                });


        return ret;

    }
}
