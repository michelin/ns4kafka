package com.michelin.ns4kafka.utils.exceptions.error;

import static com.michelin.ns4kafka.models.Kind.CONNECTOR;
import static com.michelin.ns4kafka.models.Kind.SCHEMA;
import static com.michelin.ns4kafka.utils.BytesUtils.BYTE;
import static com.michelin.ns4kafka.utils.BytesUtils.GIBIBYTE;
import static com.michelin.ns4kafka.utils.BytesUtils.KIBIBYTE;
import static com.michelin.ns4kafka.utils.BytesUtils.MEBIBYTE;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.connector.Connector;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Invalid field error.
 */
public class ValidationError {
    private static final String FIELD_NAME = "name";
    private static final String INVALID_FIELD = "Invalid value \"%s\" for field \"%s\": %s.";
    private static final String INVALID_FIELDS = "Invalid value \"%s/%s\" for fields \"%s/%s\": %s.";
    private static final String INVALID_EMPTY_FIELD = "Invalid empty value for field \"%s\": %s.";
    private static final String INVALID_EMPTY_FIELDS = "Invalid empty value for fields \"%s\": %s.";
    private static final String INVALID_RESOURCE = "Invalid \"%s\": %s.";
    private static final String INVALID_OPERATION = "Invalid \"%s\" operation: %s.";

    public static String invalidAclCollision(String invalidFieldValue, String collidingAclName) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            String.format("collision with existing ACL %s", collidingAclName));
    }

    public static String invalidAclField(String invalidFieldName, String invalidFieldValue, String allowedAclTypes) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("value must be one of %s", allowedAclTypes));
    }

    public static String invalidAclResourceType(String invalidFieldValue, String allowedAclTypes) {
        return invalidAclField("resourceType", invalidFieldValue, allowedAclTypes);
    }

    public static String invalidAclPermission(String invalidFieldValue, String allowedPermissions) {
        return String.format(INVALID_FIELD, invalidFieldValue, "permission",
            String.format("value must be one of %s", allowedPermissions));
    }

    public static String invalidAclPatternType(String invalidFieldValue, String allowedPatternTypes) {
        return invalidAclField("resourcePatternType", invalidFieldValue, allowedPatternTypes);
    }

    public static String invalidAclGrantedToMyself(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "grantedTo",
            "cannot grant ACL to yourself");
    }

    public static String invalidAclNotOwnerOfTopLevel(String invalidFieldValue,
                                                      AccessControlEntry.ResourcePatternType invalidFieldValue2) {
        return String.format(INVALID_FIELDS, invalidFieldValue, invalidFieldValue2, "resource", "resourcePatternType",
            "cannot grant ACL to yourself");
    }

    public static String invalidAclDeleteOnlyAdmin(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME, "only administrators can delete this ACL");
    }

    public static String invalidConnectClusterNameAlreadyExistGlobally(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "resource",
            "a Kafka Connect is already defined globally with this name. Please provide a different name.");
    }

    public static String invalidConnectClusterNotHealthy(String errorMessage) {
        return String.format(INVALID_RESOURCE, "connect cluster",
            String.format("the Kafka Connect is not healthy (%s)", errorMessage));
    }

    public static String invalidConnectClusterEncryption() {
        return String.format(INVALID_EMPTY_FIELDS, String.join(", ", "aes256Key", "aes256Salt"),
            "AES key and salt are required to activate encryption");
    }

    public static String invalidConnectClusterNotAllowed(String invalidFieldValue,
                                                         String allowedConnectClusters) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            String.format("value must be one of %s", allowedConnectClusters));
    }

    public static String invalidConnectClusterMalformedUrl(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "url", "malformed URL");
    }

    public static String invalidConnectClusterDeleteOperation(String connectCluster, List<Connector> connectors) {
        return String.format(INVALID_OPERATION, "delete",
            String.format("The Kafka Connect %s has %s deployed connector(s): %s. "
                    + "Please remove the associated connector(s) before deleting it.", connectCluster, connectors.size(),
                connectors.stream()
                    .map(connector ->
                        connector.getMetadata().getName())
                    .collect(Collectors.joining(", "))));
    }

    public static String invalidConnectClusterNotAllowed(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "connect-cluster",
            "namespace is not allowed to use this Kafka Connect");
    }

    public static String invalidNameEmpty() {
        return String.format(INVALID_EMPTY_FIELD, FIELD_NAME, "value must not be empty");
    }

    public static String invalidNameLength(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME, "value must not be longer than 249");
    }

    public static String invalidNameSpecChars(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            "value must only contain ASCII alphanumerics, '.', '_' or '-'");
    }

    public static String invalidOperationQuota(String quota, long used, long limit) {
        return String.format(INVALID_OPERATION, "apply",
            String.format("exceeding quota for %s: %s/%s (used/limit)", quota, used, limit));
    }

    public static String invalidNewQuota(String invalidFieldName, String invalidFieldValue, String used) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("quota already exceeded (%s/%s)", used, invalidFieldValue));
    }

    public static String invalidQuotaFormat(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("value must end with either %s, %s, %s or %s", BYTE, KIBIBYTE, MEBIBYTE, GIBIBYTE));
    }

    public static String invalidTopicName(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME, "value must not be \".\" or \"..\"");
    }

    public static String invalidTopicSpec(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            "configuration is not allowed");
    }

    public static String invalidTopicCollide(String invalidFieldValue, String collidingTopic) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            String.format("collision with existing topic %s", collidingTopic));
    }

    public static String invalidTopicTags(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "tags", "tags are not currently supported");
    }

    public static String invalidTopicCleanupPolicy(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "cleanup.policy",
            "altering topic cleanup policy from delete to compact is not currently supported in Confluent Cloud. "
                + "Please create a new topic with compact policy instead.");
    }

    public static String invalidTopicDeleteRecords() {
        return String.format(INVALID_OPERATION, "delete records",
            "cannot delete records on a compacted topic. Please delete and recreate the topic.");
    }

    public static String invalidFieldValidationNull(String invalidFieldName) {
        return String.format(INVALID_EMPTY_FIELD, invalidFieldName, "value must not be null");
    }

    public static String invalidFieldValidationEmpty(String invalidFieldName) {
        return String.format(INVALID_EMPTY_FIELD, invalidFieldName, "string must not be empty");
    }

    public static String invalidFieldValidationNumber(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName, "value must be a number");
    }

    public static String invalidFieldValidationAtLeast(String invalidFieldName, String invalidFieldValue, Number min) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("value must be at least %s", min));
    }

    public static String invalidFieldValidationAtMost(String invalidFieldName, String invalidFieldValue, Number max) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("value must be no more than %s", max));
    }

    public static String invalidFieldValidationOneOf(String invalidFieldName, String invalidFieldValue,
                                                     String allowedValues) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            String.format("string must be one of %s", allowedValues));
    }

    public static String invalidConnectorRemote(String remoteError) {
        return String.format(INVALID_RESOURCE, CONNECTOR, remoteError);
    }

    public static String invalidConnectorNoPlugin(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "connector.class",
            String.format("failed to find any class that implements connector and which name matches %s",
                invalidFieldValue));
    }

    public static String invalidConnectorEmptyConnectorClass() {
        return invalidFieldValidationNull("connector.class");
    }

    public static String invalidConnectorConnectCluster(String invalidFieldValue, String allowedConnectClusters) {
        return String.format(INVALID_FIELD, invalidFieldValue, "connectCluster",
            String.format("value must be one of %s", allowedConnectClusters));
    }

    public static String invalidConsumerGroupTopic(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "topic",
            "value must match [*, <topic>, <topic:partition>]");
    }

    public static String invalidConsumerGroupShiftBy(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "shift-by",
            "value must be an integer");
    }

    public static String invalidConsumerGroupDuration(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "by-duration",
            "value must be an ISO 8601 Duration [ PnDTnHnMnS ]");
    }

    public static String invalidConsumerGroupDatetime(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "to-datetime",
            "value must be an ISO 8601 DateTime with Time zone [ yyyy-MM-dd'T'HH:mm:ss.SSSXXX ]");
    }

    public static String invalidConsumerGroupOffsetNegative(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "to-offset",
            "value must be >= 0");
    }

    public static String invalidConsumerGroupOffsetInteger(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "to-offset",
            "value must be an integer");
    }

    public static String invalidConsumerGroupOperation(String consumerGroup, String state) {
        return String.format(INVALID_OPERATION, "reset offset",
            String.format("assignments can only be reset if the consumer group %s "
                + "is inactive but the current state is %s", consumerGroup, state));
    }

    public static String invalidImmutableValue(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            "value is immutable");
    }

    public static String invalidNamespaceNoCluster(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "cluster",
            "cluster does not exist");
    }

    public static String invalidNamespaceNoConnectCluster(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "connectClusters",
            "connect cluster does not exist");
    }

    public static String invalidNamespaceDeleteOperation(String resourceName) {
        return String.format(INVALID_OPERATION, "delete",
            String.format("namespace resource %s must be deleted first", resourceName));
    }

    public static String invalidNamespaceUserAlreadyExist(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "kafkaUser",
            "user already exists in another namespace");
    }

    public static String invalidOwner(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            "namespace is not owner of the resource");
    }

    public static String invalidOwner(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            "namespace is not owner of the resource");
    }

    public static String invalidNotFound(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            "resource not found");
    }

    public static String invalidNotFound(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName,
            "resource not found");
    }

    public static String invalidSchemaSuffix(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, FIELD_NAME,
            "value must end with -key or -value");
    }

    public static String invalidSchemaReference(String invalidFieldValue, String version) {
        return String.format(INVALID_FIELD, invalidFieldValue, "references",
            String.format("subject %s version %s not found", invalidFieldValue, version));
    }

    public static String invalidSchemaResource(String message) {
        return String.format(INVALID_RESOURCE, SCHEMA, message);
    }

    public static String invalidKafkaUser(String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, "user",
            "user does not belong to namespace");
    }
}
