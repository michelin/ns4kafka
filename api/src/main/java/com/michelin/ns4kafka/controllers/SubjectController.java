package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.SubjectService;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityRequest;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Subjects")
@Controller(value = "/api/namespaces/{namespace}/subjects")
@ExecuteOn(TaskExecutors.IO)
public class SubjectController extends NamespacedResourceController {
    /**
     * Subject service
     */
    @Inject
    SubjectService subjectService;

    /**
     * Get all the subjects within a given namespace
     *
     * @param namespace The namespace
     * @return A list of subjects
     */
    @Get
    public List<Subject> getAllByNamespace(String namespace) {
        return this.subjectService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get the last version of a subject by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A subject
     */
    @Get("/{subject}")
    public Optional<Subject> getByNamespaceAndSubject(String namespace, @PathVariable String subject) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.subjectService.isNamespaceOwnerOfSubject(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SUBJECT.toString(),
                    subject);
        }

        return this.subjectService.findByName(getNamespace(namespace), subject);
    }

    /**
     * Publish a subject to the subjects technical topic
     *
     * @param namespace The namespace
     * @param subject The subject to create
     * @param dryrun Does the creation is a dry run
     * @return The created subject
     */
    @Post
    public HttpResponse<Subject> apply(String namespace, @Valid @Body Subject subject, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.subjectService.isNamespaceOwnerOfSubject(retrievedNamespace, subject.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject.getMetadata().getName() +
                    " : namespace not owner of this subject"), subject.getKind(), subject.getMetadata().getName());
        }

        // If a compatibility is specified in the yml, apply it if different from the current one
        boolean changeCompatibility = false;
        HttpResponse<SubjectCompatibilityResponse> oldCompatibility = null;
        if (subject.getSpec().getCompatibility() != null && StringUtils.isNotBlank(subject.getSpec().getCompatibility().toString())) {
            oldCompatibility = this.subjectService.getCurrentCompatibilityBySubject(retrievedNamespace.getMetadata().getCluster(), subject);

            changeCompatibility = oldCompatibility.getBody().isPresent() &&
                    !oldCompatibility.getBody().get().compatibilityLevel().equals(subject.getSpec().getCompatibility());

            if (changeCompatibility) {
                this.subjectService.updateSubjectCompatibility(retrievedNamespace.getMetadata().getCluster(), subject,
                        SubjectCompatibilityRequest.builder()
                                .compatibility(subject.getSpec().getCompatibility())
                                .build());
            }
        }

        // Check the current compatibility of the new subject
        List<String> errorsValidateSubjectCompatibility = this.subjectService
                .validateSubjectCompatibility(retrievedNamespace.getMetadata().getCluster(), subject);

        if (!errorsValidateSubjectCompatibility.isEmpty()) {
            throw new ResourceValidationException(errorsValidateSubjectCompatibility, subject.getKind(), subject.getMetadata().getName());
        }

        subject.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        subject.getMetadata().setCluster(retrievedNamespace.getMetadata().getCluster());
        subject.getMetadata().setNamespace(retrievedNamespace.getMetadata().getName());
        subject.setStatus(Subject.SubjectStatus.ofPending());

        Optional<Subject> existingSubject = this.subjectService.findByName(retrievedNamespace,
                subject.getMetadata().getName());

        // Do nothing if subjects are equals and existing subject is not in deletion
        // If the subject is in deletion, we want to publish it again even if no change is detected
        if (existingSubject.isPresent() && existingSubject.get().equals(subject) &&
                !"to_delete".equals(existingSubject.get().getMetadata().getFinalizer())) {
            return formatHttpResponse(existingSubject.get(), ApplyStatus.unchanged);
        }

        if (dryrun) {
            // If the compatibility has been changed, rollback it
            if (changeCompatibility) {
                this.subjectService.updateSubjectCompatibility(retrievedNamespace.getMetadata().getCluster(), subject,
                        SubjectCompatibilityRequest.builder()
                                .compatibility(oldCompatibility.getBody().get().compatibilityLevel())
                                .build());
            }

            return formatHttpResponse(subject, ApplyStatus.created);
        }

        ApplyStatus status = existingSubject.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        super.sendEventLog(subject.getKind(), subject.getMetadata(), status,
                existingSubject.<Object>map(Subject::getSpec).orElse(null), subject.getSpec());

        return formatHttpResponse(this.subjectService.create(subject), status);
    }

    /**
     * Hard/soft delete all the subjects under the given subject
     *
     * @param namespace The current namespace
     * @param subject The current subject to delete
     * @param dryrun Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public HttpResponse<Void> deleteBySubject(String namespace, @PathVariable String subject,
                                              @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.subjectService.isNamespaceOwnerOfSubject(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this schema"), AccessControlEntry.ResourceType.SUBJECT.toString(),
                    subject);
        }

        Optional<Subject> existingSubjectOptional = this.subjectService.findByName(retrievedNamespace,
                subject);

        if (existingSubjectOptional.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        Subject existingSubject = existingSubjectOptional.get();
        existingSubject.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        existingSubject.getMetadata().setFinalizer("to_delete");

        super.sendEventLog(existingSubject.getKind(), existingSubject.getMetadata(), ApplyStatus.deleted,
                existingSubjectOptional.<Object>map(Subject::getSpec).orElse(null), existingSubject.getSpec());

        this.subjectService.delete(existingSubject);

        return HttpResponse.noContent();
    }
}
