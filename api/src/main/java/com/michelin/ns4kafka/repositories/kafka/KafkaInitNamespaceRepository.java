package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.InitNamespaceRepository;
import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Singleton
public class KafkaInitNamespaceRepository implements InitNamespaceRepository {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaInitNamespaceRepository.class);

    private Admin adminClient;

    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    List<ResourceType> validResourceTypes = List.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID);

    public KafkaInitNamespaceRepository(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) throws MalformedURLException {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient() {
        if (this.adminClient == null) {
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    @Override
    public Namespace getNamespace(String namespaceName, String cluster, String user) {
        // get metadata
        ObjectMeta objectMeta = buildObjectMeta(null, namespaceName, cluster);

        // get spec
        Namespace.NamespaceSpec namespaceSpec = Namespace.NamespaceSpec.builder()
                .kafkaUser(user)
                .topicValidator(TopicValidator.makeDefault(kafkaAsyncExecutorConfig.getValidator()))
                .connectValidator(ConnectValidator.makeDefault(kafkaAsyncExecutorConfig.getValidator()))
                .build();

        // build namespace
        Namespace namespace = Namespace.builder()
                .metadata(objectMeta)
                .spec(namespaceSpec)
                .build();

        return namespace;
    }

    @Override
    public RoleBinding getRoleBindings(String namespace, String cluster) {

        // get metadata
        ObjectMeta objectMeta = buildObjectMeta(namespace, StringUtils.join(namespace, "-role-binding"), cluster);

        // get spec
        // get default role
        List<String> resourceTypes = (List<String>) kafkaAsyncExecutorConfig.getValidator().get("role-binding.ressource-type");
        List<RoleBinding.Verb> verbs = new ArrayList<>();
        for (String verb : (List<String>) kafkaAsyncExecutorConfig.getValidator().get("role-binding.verbs")) {
            verbs.add(RoleBinding.Verb.valueOf(verb));
        }
        RoleBinding.RoleBindingSpec roleBindingSpec = RoleBinding.RoleBindingSpec.builder()
                .role(
                        RoleBinding.Role.builder()
                                .resourceTypes(resourceTypes)
                                .verbs(verbs)
                                .build())
                .subject(RoleBinding.Subject.builder()
                        .subjectName("TO DEFINE")
                        .subjectType(RoleBinding.SubjectType.GROUP)
                        .build())
                .build();

        // build Role binding
        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(objectMeta)
                .spec(roleBindingSpec)

                .build();

        return roleBinding;
    }

    @Override
    public List<AccessControlEntry> getAcls(String namespace, String userName, String cluster, String prefix) throws ExecutionException, InterruptedException, TimeoutException {

        List<AccessControlEntry> results = new ArrayList<>();

        AtomicInteger aclNumber = new AtomicInteger();
        // build Acl for user and namespace
        results.add(buildAccessControlEntry(namespace, new AclBinding(new ResourcePattern(ResourceType.TOPIC, prefix.toLowerCase(),
                PatternType.PREFIXED), new org.apache.kafka.common.acl.AccessControlEntry("*", "*", AclOperation.ALL, AclPermissionType.ALLOW)), cluster, aclNumber.incrementAndGet()));
        results.add(buildAccessControlEntry(namespace, new AclBinding(new ResourcePattern(ResourceType.TOPIC, prefix.toUpperCase(),
                PatternType.PREFIXED), new org.apache.kafka.common.acl.AccessControlEntry("*", "*", AclOperation.ALL, AclPermissionType.ALLOW)), cluster, aclNumber.incrementAndGet()));
        results.add(buildAccessControlEntry(namespace, new AclBinding(new ResourcePattern(ResourceType.GROUP, prefix.toLowerCase(),
                PatternType.PREFIXED), new org.apache.kafka.common.acl.AccessControlEntry("*", "*", AclOperation.ALL, AclPermissionType.ALLOW)), cluster, aclNumber.incrementAndGet()));
        results.add(buildAccessControlEntry(namespace, new AclBinding(new ResourcePattern(ResourceType.GROUP, prefix.toUpperCase(),
                PatternType.PREFIXED), new org.apache.kafka.common.acl.AccessControlEntry("*", "*", AclOperation.ALL, AclPermissionType.ALLOW)), cluster, aclNumber.incrementAndGet()));
        results.add(buildAccessControlEntry(namespace, prefix.toLowerCase(), PatternType.PREFIXED.name(),
                AccessControlEntry.ResourceType.CONNECT.name(), cluster, aclNumber.incrementAndGet()));
        results.add(buildAccessControlEntry(namespace, prefix.toUpperCase(), PatternType.PREFIXED.name(),
                AccessControlEntry.ResourceType.CONNECT.name(), cluster, aclNumber.incrementAndGet()));

        // get existing Acl for user
        Set<AclBinding> userACLs = getAdminClient()
                .describeAcls(AclBindingFilter.ANY)
                .values().get(10, TimeUnit.SECONDS)
                .stream()
                .filter(aclBinding -> validResourceTypes.contains(aclBinding.pattern().resourceType())
                        && StringUtils.equals(aclBinding.entry().principal(), "User:" + userName)
                )
                .collect(Collectors.toSet());

        // build access control entry for each Acl
        userACLs.stream()
                .filter(v -> {
                    if (v.pattern().patternType().equals(PatternType.LITERAL) && v.pattern().resourceType().equals(ResourceType.TOPIC)
                            && v.entry().operation().equals(AclOperation.READ)) {
                        return false;
                    }
                    return true;
                })
                .forEach((v -> {
                    String name = v.pattern().name();
                    if (ResourceType.TOPIC.equals(v.pattern().resourceType())) {
                        if (!StringUtils.startsWithIgnoreCase(name, prefix)) {
                            results.add(buildAccessControlEntry(namespace, v, cluster, aclNumber.incrementAndGet()));
                        }
                    } else if (ResourceType.GROUP.equals(v.pattern().resourceType())) {
                        if (!StringUtils.startsWithIgnoreCase(name, prefix)) {
                            results.add(buildAccessControlEntry(namespace, v, cluster, aclNumber.incrementAndGet()));
                        }
                    }
                }));
        return results;
    }

    /**
     * @param namespace
     * @param name
     * @param cluster
     * @return
     */
    private ObjectMeta buildObjectMeta(String namespace, String name, String cluster) {
        ObjectMeta objectMeta = ObjectMeta.builder()
                .namespace(namespace)
                .name(name)
                .cluster(cluster)
                .build();
        return objectMeta;
    }


    /**
     * @param namespace
     * @param aclBinding
     * @return
     */
    private AccessControlEntry buildAccessControlEntry(String namespace, AclBinding aclBinding, String cluster, int aclNumber) {
        AccessControlEntry accessControlEntry = buildAccessControlEntry(namespace,
                aclBinding.pattern().name(),
                aclBinding.pattern().patternType().name(),
                aclBinding.pattern().resourceType().name(),
                cluster,
                aclNumber);
        return accessControlEntry;
    }

    /**
     * @param namespace
     * @param patternName
     * @param patternType
     * @param resourceType
     * @param cluster
     * @param aclNumber
     * @return
     */
    private AccessControlEntry buildAccessControlEntry(String namespace, String patternName, String patternType, String resourceType, String cluster, int aclNumber) {

        // build access control entry
        ObjectMeta objectMeta = buildObjectMeta(namespace, StringUtils.join("acl-", namespace, "-", aclNumber), cluster);

        AccessControlEntry.AccessControlEntrySpec accessControlEntrySpec =
                AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource(patternName)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.valueOf(patternType))
                        .resourceType(AccessControlEntry.ResourceType.valueOf(resourceType))
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo(namespace)
                        .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(objectMeta)
                .spec(accessControlEntrySpec)
                .build();
        return accessControlEntry;

    }

}
