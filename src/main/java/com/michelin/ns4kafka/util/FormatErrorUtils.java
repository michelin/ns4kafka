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

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
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
        return String.format(
                INVALID_FIELD,
                invalidAclName,
                FIELD_NAME,
                String.format("collision with existing ACL %s", collidingAclName));
    }

    /**
     * Invalid ACL delete authorized only by admin.
     *
     * @param invalidAclName the invalid ACL name
     * @return the error message
     */
    public static String invalidAclDeleteOnlyAdmin(String invalidAclName) {
        return String.format(INVALID_FIELD, invalidAclName, FIELD_NAME, "only administrators can delete this ACL");
    }

    /**
     * Invalid self-assigned ACL delete, authorized only by admin.
     *
     * @param invalidAclName the invalid ACL name
     * @return the error message
     */
    public static String invalidSelfAssignedAclDelete(String invalidAclName, String acls) {
        return String.format(
                INVALID_FIELD,
                invalidAclName,
                FIELD_NAME,
                "only administrators can delete the following self-assigned ACLs: " + acls);
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
        return String.format(
            INVALID_FIELD,
            invalidFieldValue,
            invalidFieldName,
            String.format("value must be one of \"%s\"", allowedFieldTypes));
    }

    /**
     * Invalid ACL granted to myself.
     *
     * @param invalidGrantedToValue the invalid granted to value
     * @return the error message
     */
    public static String invalidAclGrantedToMyself(String invalidGrantedToValue) {
        return String.format(INVALID_FIELD, invalidGrantedToValue, "grantedTo", "cannot grant ACL to yourself");
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
        return String.format(
                INVALID_FIELDS,
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
        return String.format(
                INVALID_OPERATION,
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
        return String.format(
                INVALID_EMPTY_FIELDS,
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
        return String.format(
                INVALID_FIELD,
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
        return String.format(
                INVALID_EMPTY_FIELDS,
                String.join(", ", "aes256Key", "aes256Salt"),
                "Both AES key and salt are required for encryption");
    }

    /**
     * Invalid connect cluster not healthy.
     *
     * @param name the name
     * @param errorMessage the error message
     * @return the error message
     */
    public static String invalidConnectClusterNotHealthy(String name, String errorMessage) {
        return String.format(
                INVALID_RESOURCE,
                name,
                String.format("the Kafka Connect is not healthy (%s)", errorMessage.toLowerCase()));
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
        return String.format(
                INVALID_FIELD,
                invalidConnectorClassValue,
                "connector.class",
                String.format(
                        "failed to find any class that implements connector and which name matches %s",
                        invalidConnectorClassValue));
    }

    /**
     * Invalid connector remote error.
     *
     * @param name the name
     * @param remoteError the remote error
     * @return the error message
     */
    public static String invalidConnectorRemote(String name, String remoteError) {
        return String.format(INVALID_RESOURCE, name, remoteError.toLowerCase());
    }

    /**
     * Invalid datetime value.
     *
     * @param invalidDateTimeValue the invalid date time value
     * @return the error message
     */
    public static String invalidConsumerGroupDatetime(String invalidDateTimeValue) {
        return String.format(
                INVALID_FIELD,
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
        return String.format(
                INVALID_FIELD,
                invalidDurationValue,
                "by-duration",
                "value must be an ISO 8601 duration [ PnDTnHnMnS ]");
    }

    /**
     * Invalid offset value.
     *
     * @param invalidOffsetValue the invalid offset value
     * @return the error message
     */
    public static String invalidConsumerGroupOffsetInteger(String invalidOffsetValue) {
        return String.format(INVALID_FIELD, invalidOffsetValue, "to-offset", "value must be an integer");
    }

    /**
     * Invalid offset value.
     *
     * @param invalidOffsetValue the invalid offset value
     * @return the error message
     */
    public static String invalidConsumerGroupOffsetNegative(String invalidOffsetValue) {
        return String.format(INVALID_FIELD, invalidOffsetValue, "to-offset", "value must be >= 0");
    }

    /**
     * Invalid operation on consumer group.
     *
     * @param consumerGroup the consumer group
     * @param state the state
     * @return the error message
     */
    public static String invalidConsumerGroupOperation(String consumerGroup, String state) {
        return String.format(
                INVALID_OPERATION,
                "reset offset",
                String.format(
                        "assignments can only be reset if the consumer group \"%s\" "
                                + "is inactive but the current state is %s",
                        consumerGroup, state));
    }

    /**
     * Invalid shift by value.
     *
     * @param invalidShiftByValue the invalid shift by value
     * @return the error message
     */
    public static String invalidConsumerGroupShiftBy(String invalidShiftByValue) {
        return String.format(INVALID_FIELD, invalidShiftByValue, "shift-by", "value must be an integer");
    }

    /**
     * Invalid topic value.
     *
     * @param invalidTopicValue the invalid topic value
     * @return the error message
     */
    public static String invalidConsumerGroupTopic(String invalidTopicValue) {
        return String.format(
                INVALID_FIELD, invalidTopicValue, "topic", "value must match [*, <topic>, <topic:partition>]");
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
        return String.format(
                INVALID_FIELD, invalidFieldValue, invalidFieldName, String.format("value must be at least %s", min));
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
        return String.format(
                INVALID_FIELD,
                invalidFieldValue,
                invalidFieldName,
                String.format("value must be no more than %s", max));
    }

    /**
     * Invalid field must not be empty.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidFieldValidationEmpty(String invalidFieldName) {
        return String.format(INVALID_EMPTY_FIELD, invalidFieldName, "string must not be empty");
    }

    /**
     * Invalid field must not be null.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidFieldValidationNull(String invalidFieldName) {
        return String.format(INVALID_EMPTY_FIELD, invalidFieldName, "value must not be null");
    }

    /**
     * Invalid field must be a number.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidFieldValidationNumber(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName, "value must be a number");
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
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @param mandatoryFieldValues the mandatory field values
     * @return the error message
     */
    public static String invalidFieldValidationContains(
        String invalidFieldName, String invalidFieldValue, String mandatoryFieldValues) {
        return String.format(
            INVALID_FIELD,
            invalidFieldValue,
            invalidFieldName,
            String.format("value must contain \"%s\"", mandatoryFieldValues));
    }

    /**
     * Invalid field is immutable.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidImmutableValue(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName, "value is immutable");
    }

    /**
     * Invalid field is immutable.
     *
     * @param invalidFieldName the invalid field name
     * @return the error message
     */
    public static String invalidImmutableField(String invalidFieldName) {
        return String.format(
                INVALID_OPERATION, OPERATION_APPLY, String.format("field \"%s\" is immutable", invalidFieldName));
    }

    /**
     * Invalid Kafka user.
     *
     * @param invalidKafkaUserValue the invalid field value
     * @return the error message
     */
    public static String invalidKafkaUser(String invalidKafkaUserValue) {
        return String.format(INVALID_FIELD, invalidKafkaUserValue, "user", "user does not belong to namespace");
    }

    /**
     * Invalid namespace delete operation.
     *
     * @param resourceName the resource name
     * @return the error message
     */
    public static String invalidNamespaceDeleteOperation(String resourceName) {
        return String.format(
                INVALID_OPERATION,
                "delete",
                String.format("namespace resource %s must be deleted first", resourceName));
    }

    /**
     * Invalid connect cluster not exist.
     *
     * @param invalidConnectClusterValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceNoConnectCluster(String invalidConnectClusterValue) {
        return String.format(
                INVALID_FIELD, invalidConnectClusterValue, "connectClusters", "connect cluster does not exist");
    }

    /**
     * Invalid cluster not exist.
     *
     * @param invalidClusterValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceNoCluster(String invalidClusterValue) {
        return String.format(INVALID_FIELD, invalidClusterValue, "cluster", "cluster does not exist");
    }

    /**
     * Invalid namespace topic validator key confluent cloud.
     *
     * @param invalidKey the invalid key
     * @return the error message
     */
    public static String invalidNamespaceTopicValidatorKeyConfluentCloud(String invalidKey) {
        return String.format(
                INVALID_FIELD,
                invalidKey,
                "validationConstraints",
                "configuration not editable on a Confluent Cloud cluster");
    }

    /**
     * Invalid Kafka user already exist.
     *
     * @param invalidKafkaValue the invalid field value
     * @return the error message
     */
    public static String invalidNamespaceUserAlreadyExist(String invalidKafkaValue) {
        return String.format(INVALID_FIELD, invalidKafkaValue, "kafkaUser", "user already exists in another namespace");
    }

    /**
     * Invalid name empty.
     *
     * @return the error message
     */
    public static String invalidNameEmpty() {
        return String.format(INVALID_EMPTY_FIELD, FIELD_NAME, "value must not be empty");
    }

    /**
     * Invalid name length.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidNameLength(String invalidNameValue) {
        return String.format(INVALID_FIELD, invalidNameValue, FIELD_NAME, "value must not be longer than 249");
    }

    /**
     * Invalid name spec chars.
     *
     * @param invalidNameValue the invalid field value
     * @return the error message
     */
    public static String invalidNameSpecChars(String invalidNameValue) {
        return String.format(
                INVALID_FIELD,
                invalidNameValue,
                FIELD_NAME,
                "value must only contain ASCII alphanumerics, '.', '_' or '-'");
    }

    /**
     * Invalid resource not found.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidNotFound(String invalidFieldName, String invalidFieldValue) {
        return String.format(INVALID_FIELD, invalidFieldValue, invalidFieldName, "resource not found");
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
        return String.format(
                INVALID_FIELD, invalidFieldValue, invalidFieldName, "namespace is not owner of the resource");
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
        return String.format(
                INVALID_FIELD,
                invalidQuotaValue,
                invalidQuotaName,
                String.format("value must end with either %s, %s, %s or %s", BYTE, KIBIBYTE, MEBIBYTE, GIBIBYTE));
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
        return String.format(
                INVALID_FIELD,
                invalidQuotaValue,
                invalidQuotaName,
                String.format("quota already exceeded (%s/%s)", used, invalidQuotaValue));
    }

    /**
     * Invalid reset password provider.
     *
     * @param provider the provider
     * @return the error message
     */
    public static String invalidResetPasswordProvider(ManagedClusterProperties.KafkaProvider provider) {
        return String.format(
                INVALID_OPERATION,
                "reset password",
                String.format("Password reset is not available with provider %s", provider));
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
        return String.format(
                INVALID_OPERATION,
                OPERATION_APPLY,
                String.format("exceeding quota for %s: %s/%s (used/limit)", quota, used, limit));
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
        return String.format(
                INVALID_OPERATION,
                OPERATION_APPLY,
                String.format("exceeding quota for %s: %s/%s (used/limit). Cannot add %s", quota, used, limit, toAdd));
    }

    /**
     * Invalid protected namespace grant ACL to basic namespaces.
     *
     * @return the error message
     */
    public static String invalidProtectedNamespaceGrantAcl() {
        return String.format(
                INVALID_OPERATION, OPERATION_APPLY, "protected namespace can only grant ACL to protected namespaces");
    }

    /**
     * Invalid protected namespace grant ACL to basic namespaces.
     *
     * @return the error message
     */
    public static String invalidProtectedNamespaceGrantPublicAcl() {
        return String.format(INVALID_OPERATION, OPERATION_APPLY, "protected namespace can not grant public ACL");
    }

    /**
     * Invalid schema suffix.
     *
     * @param invalidNameValue the invalid name value
     * @return the error message
     */
    public static String invalidSchemaSuffix(String invalidNameValue) {
        return String.format(INVALID_FIELD, invalidNameValue, FIELD_NAME, "value must end with -key or -value");
    }

    /**
     * Invalid schema reference.
     *
     * @param invalidSubjectValue the invalid subject value
     * @param invalidVersion the invalid version
     * @return the error message
     */
    public static String invalidSchemaReference(String invalidSubjectValue, String invalidVersion) {
        return String.format(
                INVALID_FIELD,
                invalidSubjectValue,
                "references",
                String.format("subject %s version %s not found", invalidSubjectValue, invalidVersion));
    }

    /**
     * Invalid schema resource validation.
     *
     * @param name the name
     * @param message the message
     * @return the error message
     */
    public static String invalidSchemaResource(String name, String message) {
        return String.format(INVALID_RESOURCE, name, message.toLowerCase());
    }

    /**
     * Invalid topic cleanup policy.
     *
     * @param invalidCleanupPolicyValue the invalid cleanup policy value
     * @return the error message
     */
    public static String invalidTopicCleanupPolicy(String invalidCleanupPolicyValue) {
        return String.format(
                INVALID_FIELD,
                invalidCleanupPolicyValue,
                "cleanup.policy",
                "altering topic cleanup policy from delete to compact is not currently supported in Confluent Cloud. "
                        + "Please create a new topic with compact policy instead");
    }

    /**
     * Invalid topic collision.
     *
     * @param invalidNameValue the invalid name value
     * @param collidingTopicName the colliding topic name
     * @return the error message
     */
    public static String invalidTopicCollide(String invalidNameValue, String collidingTopicName) {
        return String.format(
                INVALID_FIELD,
                invalidNameValue,
                FIELD_NAME,
                String.format("collision with existing topic %s", collidingTopicName));
    }

    /**
     * Invalid topic delete records.
     *
     * @return the error message
     */
    public static String invalidTopicDeleteRecords() {
        return String.format(
                INVALID_OPERATION,
                "delete records",
                "cannot delete records on a compacted topic. Please delete and recreate the topic");
    }

    /**
     * Invalid topic name.
     *
     * @param invalidNameValue the invalid name value
     * @return the error message
     */
    public static String invalidTopicName(String invalidNameValue) {
        return String.format(INVALID_FIELD, invalidNameValue, FIELD_NAME, "value must not be \".\" or \"..\"");
    }

    /**
     * Invalid topic spec.
     *
     * @param invalidFieldName the invalid field name
     * @param invalidFieldValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicSpec(String invalidFieldName, String invalidFieldValue) {
        return String.format(
                INVALID_FIELD, invalidFieldValue, invalidFieldName, "configuration is not allowed on your namespace");
    }

    /**
     * Invalid topic tags.
     *
     * @param invalidTagValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicTags(String invalidTagValue) {
        return String.format(INVALID_FIELD, invalidTagValue, "tags", "tags are not currently supported");
    }

    /**
     * Invalid topic tags format.
     *
     * @param invalidTagFormatValue the invalid field value
     * @return the error message
     */
    public static String invalidTopicTagsFormat(String invalidTagFormatValue) {
        return String.format(
                INVALID_FIELD,
                invalidTagFormatValue,
                "tags",
                "tags should start with letter and be followed by alphanumeric or _ characters");
    }
}
