/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.util;

import static com.michelin.ns4kafka.util.BytesUtils.BYTE;
import static com.michelin.ns4kafka.util.BytesUtils.GIBIBYTE;
import static com.michelin.ns4kafka.util.BytesUtils.KIBIBYTE;
import static com.michelin.ns4kafka.util.BytesUtils.MEBIBYTE;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Format error utils. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FormatErrorUtils {
    private static final String OPERATION_APPLY = "apply";
    private static final String FIELD_NAME = "name";
    private static final String INVALID_FIELD = "Invalid value \"%s\" for field \"%s\": %s.";
    private static final String INVALID_FIELDS = "Invalid value \"%s/%s\" for fields \"%s/%s\": %s.";
    private static final String INVALID_EMPTY_FIELD = "Invalid empty value for field \"%s\": %s.";
    private static final String INVALID_EMPTY_FIELDS = "Invalid empty value for fields \"%s\": %s.";
    private static final String INVALID_RESOURCE = "Invalid \"%s\": %s.";
    private static final String INVALID_OPERATION = "Invalid \"%s\" operation: %s.";

    /**
     * Invalid ACL collision.
     *
     * @param invalidAclName the invalid ACL name
     * @param collidingAclName the colliding ACL name
     * @return the error message
     */
    public static String invalidAclCollision(String invalidAclName, String collidingAclName) {
        return INVALID_FIELD.formatted(
                invalidAclName, FIELD_NAME, "collision with existing ACL %s".formatted(collidingAclName));
    }

    /**
     * Invalid ACL delete authorized only by admin.
     *
     * @param invalidAclName the invalid ACL name
     * @return the error message
     */
    public static String invalidAclDeleteOnlyAdmin(String invalidAclName) {
        return INVALID_FIELD.formatted(invalidAclName, FIELD_NAME, "only administrators can delete this ACL");
    }

    /**
     * Invalid self-assigned ACL delete, authorized only by admin.
     *
     * @param invalidAclName the invalid ACL name
     * @return the error message
     */
    public static String invalidSelfAssignedAclDelete(String invalidAclName, String acls) {
        return INVALID_FIELD.formatted(
                invalidAclName, FIELD_NAME, "only administrators can delete the following self-assigned ACLs: " + acls);
    }

    /**
     * Invalid ACL field.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param allowedFieldTypes the allowed field types
     * @return the error message
     */
    public static String invalidValueMustBeOneOf(
            String invalidFieldName, String invalidFieldValue, String allowedFieldTypes) {
        return INVALID_FIELD.formatted(
                invalidFieldValue, invalidFieldName, "value must be one of \"%s\"".formatted(allowedFieldTypes));
    }

    /**
     * Invalid ACL granted to myself.
     *
     * @param invalidGrantedToValue the invalid granted to value
     * @return the error message
     */
    public static String invalidAclGrantedToMyself(String invalidGrantedToValue) {
        return INVALID_FIELD.formatted(invalidGrantedToValue, "grantedTo", "cannot grant ACL to yourself");
    }

    /**
     * Invalid ACL not owner of top level ACL.
     *
     * @param invalidResourceValue the invalid resource value
     * @param invalidResourcePatternTypeValue the invalid resource pattern type value
     * @return the error message
     */
    public static String invalidAclNotOwnerOfTopLevel(
            String invalidResourceValue, AccessControlEntry.ResourcePatternType invalidResourcePatternTypeValue) {
        return INVALID_FIELDS.formatted(
                invalidResourceValue,
                invalidResourcePatternTypeValue,
                "resource",
                "resourcePatternType",
                "cannot grant ACL because namespace is not owner of the top level resource");
    }

    /**
     * Invalid ACL pattern type.
     *
     * @param invalidResourcePatternTypeValue the invalid resource pattern type value
     * @param allowedPatternTypes the allowed pattern types
     * @return the error message
     */
    public static String invalidAclPatternType(String invalidResourcePatternTypeValue, String allowedPatternTypes) {
        return invalidValueMustBeOneOf("resourcePatternType", invalidResourcePatternTypeValue, allowedPatternTypes);
    }

    /**
     * Invalid ACL permission.
     *
     * @param invalidPermissionValue the invalid permission value
     * @param allowedPermissions the allowed permissions
     * @return the error message
     */
    public static String invalidAclPermission(String invalidPermissionValue, String allowedPermissions) {
        return invalidValueMustBeOneOf("permission", invalidPermissionValue, allowedPermissions);
    }

    /**
     * Invalid ACL resource type.
     *
     * @param invalidResourceTypeValue the invalid resource type value
     * @param allowedAclTypes the allowed ACL types
     * @return the error message
     */
    public static String invalidAclResourceType(String invalidResourceTypeValue, String allowedAclTypes) {
        return invalidValueMustBeOneOf("resourceType", invalidResourceTypeValue, allowedAclTypes);
    }

    /**
     * Invalid delete operation on connect cluster.
     *
     * @param connectCluster the connect cluster
     * @param connectors the connectors deployed on the connect cluster
     * @return the error message
     */
    public static String invalidConnectClusterDeleteOperation(String connectCluster, List<Connector> connectors) {
        return INVALID_OPERATION.formatted(
                "delete",
                String.format(
                        "The Kafka Connect \"%s\" has %s deployed connector(s): %s. "
                                + "Please remove the associated connector(s) before deleting it",
                        connectCluster,
                        connectors.size(),
                        connectors.stream()
                                .map(connector -> connector.getMetadata().getName())
                                .collect(Collectors.joining(", "))));
    }

    /**
     * Invalid connect cluster encryption configuration.
     *
     * @return the error message
     */
    public static String invalidConnectClusterEncryptionConfig() {
        return INVALID_EMPTY_FIELDS.formatted(
                String.join(", ", "aes256Key", "aes256Salt"),
                "Both AES key and salt are required to activate encryption");
    }

    /**
     * Invalid connect cluster name already exist globally.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidConnectClusterNameAlreadyExistGlobally(String invalidNameValue) {
        return INVALID_FIELD.formatted(
                invalidNameValue,
                FIELD_NAME,
                "a Kafka Connect is already defined globally with this name. Please provide a different name");
    }

    /**
     * Invalid connect cluster missing encryption config.
     *
     * @return the error message
     */
    public static String invalidConnectClusterNoEncryptionConfig() {
        return INVALID_EMPTY_FIELDS.formatted(
                String.join(", ", "aes256Key", "aes256Salt"), "Both AES key and salt are required for encryption");
    }

    /**
     * Invalid connect cluster not healthy.
     *
     * @param name the name
     * @param errorMessage the error message
     * @return the error message
     */
    public static String invalidConnectClusterNotHealthy(String name, String errorMessage) {
        return INVALID_RESOURCE.formatted(
                name, "the Kafka Connect is not healthy (%s)".formatted(errorMessage.toLowerCase()));
    }

    /**
     * Invalid connect cluster.
     *
     * @param invalidNameValue the invalid field value
     * @param allowedConnectClusters the allowed connect clusters
     * @return the error message
     */
    public static String invalidConnectorConnectCluster(String invalidNameValue, String allowedConnectClusters) {
        return invalidValueMustBeOneOf("connectCluster", invalidNameValue, allowedConnectClusters);
    }

    /**
     * Invalid connector class null.
     *
     * @return the error message
     */
    public static String invalidConnectorEmptyConnectorClass() {
        return invalidFieldValidationNull("connector.class");
    }

    /**
     * Invalid connector class not found on connect cluster.
     *
     * @param invalidConnectorClassValue the invalid connector class value
     * @return the error message
     */
    public static String invalidConnectorNoPlugin(String invalidConnectorClassValue) {
        return INVALID_FIELD.formatted(
                invalidConnectorClassValue,
                "connector.class",
                "failed to find any class that implements connector and which name matches %s"
                        .formatted(invalidConnectorClassValue));
    }

    /**
     * Invalid connector remote error.
     *
     * @param name the name
     * @param remoteError the remote error
     * @return the error message
     */
    public static String invalidConnectorRemote(String name, String remoteError) {
        return INVALID_RESOURCE.formatted(name, remoteError.toLowerCase());
    }

    /**
     * Invalid datetime value.
     *
     * @param invalidDateTimeValue the invalid date time value
     * @return the error message
     */
    public static String invalidConsumerGroupDatetime(String invalidDateTimeValue) {
        return INVALID_FIELD.formatted(
                invalidDateTimeValue,
                "to-datetime",
                "value must be an ISO 8601 DateTime with Time zone [ yyyy-MM-dd'T'HH:mm:ss.SSSXXX ]");
    }

    /**
     * Invalid duration value.
     *
     * @param invalidDurationValue the invalid duration value
     * @return the error message
     */
    public static String invalidConsumerGroupDuration(String invalidDurationValue) {
        return INVALID_FIELD.formatted(
                invalidDurationValue, "by-duration", "value must be an ISO 8601 duration [ PnDTnHnMnS ]");
    }

    /**
     * Invalid offset value.
     *
     * @param invalidOffsetValue the invalid offset value
     * @return the error message
     */
    public static String invalidConsumerGroupOffsetInteger(String invalidOffsetValue) {
        return INVALID_FIELD.formatted(invalidOffsetValue, "to-offset", "value must be an integer");
    }

    /**
     * Invalid offset value.
     *
     * @param invalidOffsetValue the invalid offset value
     * @return the error message
     */
    public static String invalidConsumerGroupOffsetNegative(String invalidOffsetValue) {
        return INVALID_FIELD.formatted(invalidOffsetValue, "to-offset", "value must be >= 0");
    }

    /**
     * Invalid operation on consumer group.
     *
     * @param consumerGroup the consumer group
     * @param targetState the target state
     * @param currentState the current state
     * @return the error message
     */
    public static String invalidConsumerGroupOperation(String consumerGroup, String targetState, String currentState) {
        return INVALID_OPERATION.formatted(
                "reset offset",
                String.format(
                        "offsets can only be reset if the consumer group \"%s\" "
                                + "is %s but the current state is %s. Stop the consumption and wait \"session.timeout.ms\" before retrying",
                        consumerGroup, targetState, currentState));
    }

    /**
     * Invalid shift by value.
     *
     * @param invalidShiftByValue the invalid shift by value
     * @return the error message
     */
    public static String invalidConsumerGroupShiftBy(String invalidShiftByValue) {
        return INVALID_FIELD.formatted(invalidShiftByValue, "shift-by", "value must be an integer");
    }

    /**
     * Invalid topic value.
     *
     * @param invalidTopicValue the invalid topic value
     * @return the error message
     */
    public static String invalidConsumerGroupTopic(String invalidTopicValue) {
        return INVALID_FIELD.formatted(invalidTopicValue, "topic", "value must match [*, <topic>, <topic:partition>]");
    }

    /**
     * Invalid field must be at least.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param min the min
     * @return the error message
     */
    public static String invalidFieldValidationAtLeast(String invalidFieldName, String invalidFieldValue, Number min) {
        return INVALID_FIELD.formatted(invalidFieldValue, invalidFieldName, "value must be at least %s".formatted(min));
    }

    /**
     * Invalid field must be at most.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param max the max
     * @return the error message
     */
    public static String invalidFieldValidationAtMost(String invalidFieldName, String invalidFieldValue, Number max) {
        return INVALID_FIELD.formatted(
                invalidFieldValue, invalidFieldName, "value must be no more than %s".formatted(max));
    }

    /**
     * Invalid field must not be empty.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidFieldValidationEmpty(String invalidFieldName) {
        return INVALID_EMPTY_FIELD.formatted(invalidFieldName, "string must not be empty");
    }

    /**
     * Invalid field must not be null.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidFieldValidationNull(String invalidFieldName) {
        return INVALID_EMPTY_FIELD.formatted(invalidFieldName, "value must not be null");
    }

    /**
     * Invalid field must be a number.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidFieldValidationNumber(String invalidFieldName, String invalidFieldValue) {
        return INVALID_FIELD.formatted(invalidFieldValue, invalidFieldName, "value must be a number");
    }

    /**
     * Invalid field must be one of.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param allowedValues the allowed values
     * @return the error message
     */
    public static String invalidFieldValidationOneOf(
            String invalidFieldName, String invalidFieldValue, String allowedValues) {
        return invalidValueMustBeOneOf(invalidFieldName, invalidFieldValue, allowedValues);
    }

    /**
     * Invalid field must contain values
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param mandatoryFieldValues the mandatory field values
     * @return the error message
     */
    public static String invalidFieldValidationContains(
            String invalidFieldName, String invalidFieldValue, String mandatoryFieldValues) {
        return INVALID_FIELD.formatted(
                invalidFieldValue, invalidFieldName, "value must contain \"%s\"".formatted(mandatoryFieldValues));
    }

    /**
     * Invalid field is immutable.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidImmutableValue(String invalidFieldName, String invalidFieldValue) {
        return INVALID_FIELD.formatted(invalidFieldValue, invalidFieldName, "value is immutable");
    }

    /**
     * Invalid field is immutable.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidImmutableField(String invalidFieldName) {
        return INVALID_OPERATION.formatted(OPERATION_APPLY, "field \"%s\" is immutable".formatted(invalidFieldName));
    }

    /**
     * Invalid Kafka user.
     *
     * @param invalidKafkaUserValue the invalid field value
     * @return the error message
     */
    public static String invalidKafkaUser(String invalidKafkaUserValue) {
        return INVALID_FIELD.formatted(invalidKafkaUserValue, "user", "user does not belong to namespace");
    }

    /**
     * Invalid namespace delete operation.
     *
     * @param resourceName the resource name
     * @return the error message
     */
    public static String invalidNamespaceDeleteOperation(String resourceName) {
        return INVALID_OPERATION.formatted(
                "delete", "namespace resource %s must be deleted first".formatted(resourceName));
    }

    /**
     * Invalid connect cluster not exist.
     *
     * @param invalidConnectClusterValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceNoConnectCluster(String invalidConnectClusterValue) {
        return INVALID_FIELD.formatted(invalidConnectClusterValue, "connectClusters", "connect cluster does not exist");
    }

    /**
     * Invalid cluster not exist.
     *
     * @param invalidClusterValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceNoCluster(String invalidClusterValue) {
        return INVALID_FIELD.formatted(invalidClusterValue, "cluster", "cluster does not exist");
    }

    /**
     * Invalid namespace topic validator key confluent cloud.
     *
     * @param invalidKey the invalid key
     * @return the error message
     */
    public static String invalidNamespaceTopicValidatorKeyConfluentCloud(String invalidKey) {
        return INVALID_FIELD.formatted(
                invalidKey, "validationConstraints", "configuration not editable on a Confluent Cloud cluster");
    }

    /**
     * Invalid Kafka user already exist.
     *
     * @param invalidKafkaValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceUserAlreadyExist(String invalidKafkaValue) {
        return INVALID_FIELD.formatted(invalidKafkaValue, "kafkaUser", "user already exists in another namespace");
    }

    /**
     * Invalid name empty.
     *
     * @return the error message
     */
    public static String invalidNameEmpty() {
        return INVALID_EMPTY_FIELD.formatted(FIELD_NAME, "value must not be empty");
    }

    /**
     * Invalid name length.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidNameLength(String invalidNameValue) {
        return INVALID_FIELD.formatted(invalidNameValue, FIELD_NAME, "value must not be longer than 249");
    }

    /**
     * Invalid name spec chars.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidNameSpecChars(String invalidNameValue) {
        return INVALID_FIELD.formatted(
                invalidNameValue, FIELD_NAME, "value must only contain ASCII alphanumerics, '.', '_' or '-'");
    }

    /**
     * Invalid resource not found.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidNotFound(String invalidFieldName, String invalidFieldValue) {
        return INVALID_FIELD.formatted(invalidFieldValue, invalidFieldName, "resource not found");
    }

    /**
     * Invalid resource not found.
     *
     * @param invalidNameValue the invalid name value
     * @return the error message
     */
    public static String invalidNotFound(String invalidNameValue) {
        return invalidNotFound(FIELD_NAME, invalidNameValue);
    }

    /**
     * Invalid owner.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidOwner(String invalidFieldName, String invalidFieldValue) {
        return INVALID_FIELD.formatted(invalidFieldValue, invalidFieldName, "namespace is not owner of the resource");
    }

    /**
     * Invalid owner.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidOwner(String invalidNameValue) {
        return invalidOwner(FIELD_NAME, invalidNameValue);
    }

    /**
     * Invalid quota format.
     *
     * @param invalidQuotaName the invalid quota name
     * @param invalidQuotaValue the invalid quota value
     * @return the error message
     */
    public static String invalidQuotaFormat(
            ResourceQuota.ResourceQuotaSpecKey invalidQuotaName, String invalidQuotaValue) {
        return INVALID_FIELD.formatted(
                invalidQuotaValue,
                invalidQuotaName,
                "value must end with either %s, %s, %s or %s".formatted(BYTE, KIBIBYTE, MEBIBYTE, GIBIBYTE));
    }

    /**
     * Invalid quota already exceeded.
     *
     * @param invalidQuotaName the invalid quota name
     * @param invalidQuotaValue the invalid quota value
     * @param used the used quota
     * @return the error message
     */
    public static String invalidQuotaAlreadyExceeded(
            ResourceQuota.ResourceQuotaSpecKey invalidQuotaName, String invalidQuotaValue, String used) {
        return INVALID_FIELD.formatted(
                invalidQuotaValue,
                invalidQuotaName,
                "quota already exceeded (%s/%s)".formatted(used, invalidQuotaValue));
    }

    /**
     * Invalid reset password provider.
     *
     * @param provider the provider
     * @return the error message
     */
    public static String invalidResetPasswordProvider(ManagedClusterProperties.KafkaProvider provider) {
        return INVALID_OPERATION.formatted(
                "reset password", "Password reset is not available with provider %s".formatted(provider));
    }

    /**
     * Invalid quota operation.
     *
     * @param quota the quota
     * @param used the used quota
     * @param limit the limit quota
     * @return the error message
     */
    public static String invalidQuotaOperation(ResourceQuota.ResourceQuotaSpecKey quota, long used, long limit) {
        return INVALID_OPERATION.formatted(
                OPERATION_APPLY, "exceeding quota for %s: %s/%s (used/limit)".formatted(quota, used, limit));
    }

    /**
     * Invalid quota operation adding data information.
     *
     * @param quota the quota
     * @param used the used quota
     * @param limit the limit quota
     * @param toAdd the data to add
     * @return the error message
     */
    public static String invalidQuotaOperationCannotAdd(
            ResourceQuota.ResourceQuotaSpecKey quota, String used, String limit, String toAdd) {
        return INVALID_OPERATION.formatted(
                OPERATION_APPLY,
                "exceeding quota for %s: %s/%s (used/limit). Cannot add %s".formatted(quota, used, limit, toAdd));
    }

    /**
     * Invalid protected namespace grant ACL to basic namespaces.
     *
     * @return the error message
     */
    public static String invalidProtectedNamespaceGrantAcl() {
        return INVALID_OPERATION.formatted(
                OPERATION_APPLY, "protected namespace can only grant ACL to protected namespaces");
    }

    /**
     * Invalid protected namespace grant ACL to basic namespaces.
     *
     * @return the error message
     */
    public static String invalidProtectedNamespaceGrantPublicAcl() {
        return INVALID_OPERATION.formatted(OPERATION_APPLY, "protected namespace can not grant public ACL");
    }

    /**
     * Invalid schema subject name for the configured naming strategy.
     *
     * @param subjectName the subject name
     * @param strategies the configured naming strategies
     * @return the error message
     */
    public static String invalidSchemaSubjectName(String subjectName, List<SubjectNameStrategy> strategies) {
        String formattedStrategies = strategies
                .stream()
                .map(SubjectNameStrategy::toShortName)
                .collect(Collectors.joining(", "));

        return String.format(
                INVALID_FIELD,
                subjectName,
                FIELD_NAME,
                String.format("value must follow authorized strategies: %s", formattedStrategies));
    }

    /**
     * Invalid schema reference.
     *
     * @param invalidSubjectValue the invalid subject value
     * @param invalidVersion the invalid version
     * @return the error message
     */
    public static String invalidSchemaReference(String invalidSubjectValue, String invalidVersion) {
        return INVALID_FIELD.formatted(
                invalidSubjectValue,
                "references",
                "subject %s version %s not found".formatted(invalidSubjectValue, invalidVersion));
    }

    /**
     * Invalid schema resource validation.
     *
     * @param name the name
     * @param message the message
     * @return the error message
     */
    public static String invalidSchemaResource(String name, String message) {
        return INVALID_RESOURCE.formatted(name, message.toLowerCase());
    }

    /**
     * Invalid topic cleanup policy.
     *
     * @param invalidCleanUpPolicyValue the invalid cleanup policy value
     * @return the error message
     */
    public static String invalidTopicCleanUpPolicy(String invalidCleanUpPolicyValue) {
        return INVALID_FIELD.formatted(
                invalidCleanUpPolicyValue,
                CLEANUP_POLICY_CONFIG,
                "altering topic configuration from \"delete\" to \"compact\" and \"delete\" is not currently supported in Confluent Cloud. "
                        + "Please create a new topic instead");
    }

    /**
     * Invalid topic collision.
     *
     * @param invalidNameValue the invalid name value
     * @param collidingTopicName the colliding topic name
     * @return the error message
     */
    public static String invalidTopicCollide(String invalidNameValue, String collidingTopicName) {
        return INVALID_FIELD.formatted(
                invalidNameValue, FIELD_NAME, "collision with existing topic %s".formatted(collidingTopicName));
    }

    /**
     * Invalid topic delete records.
     *
     * @return the error message
     */
    public static String invalidTopicDeleteRecords() {
        return INVALID_OPERATION.formatted(
                "delete records", "cannot delete records on a compacted topic. Please delete and recreate the topic");
    }

    /**
     * Invalid topic name.
     *
     * @param invalidNameValue the invalid name value
     * @return the error message
     */
    public static String invalidTopicName(String invalidNameValue) {
        return INVALID_FIELD.formatted(invalidNameValue, FIELD_NAME, "value must not be \".\" or \"..\"");
    }

    /**
     * Invalid topic spec.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicSpec(String invalidFieldName, String invalidFieldValue) {
        return INVALID_FIELD.formatted(
                invalidFieldValue, invalidFieldName, "configuration is not allowed on your namespace");
    }

    /**
     * Invalid topic tags.
     *
     * @param invalidTagValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicTags(String invalidTagValue) {
        return INVALID_FIELD.formatted(invalidTagValue, "tags", "tags are not currently supported");
    }

    /**
     * Invalid topic tags format.
     *
     * @param invalidTagFormatValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicTagsFormat(String invalidTagFormatValue) {
        return INVALID_FIELD.formatted(
                invalidTagFormatValue,
                "tags",
                "tags should start with letter and be followed by alphanumeric or _ characters");
    }
}
