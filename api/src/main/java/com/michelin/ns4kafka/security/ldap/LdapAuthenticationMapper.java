package com.michelin.ns4kafka.security.ldap;

import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.configuration.security.ldap.ContextAuthenticationMapper;
import io.micronaut.configuration.security.ldap.DefaultContextAuthenticationMapper;
import io.micronaut.configuration.security.ldap.configuration.LdapConfiguration;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.convert.value.ConvertibleValues;
import io.micronaut.core.util.StringUtils;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UserDetails;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
@Replaces(DefaultContextAuthenticationMapper.class)
@Requires(property = LdapConfiguration.PREFIX + ".enabled", notEquals = StringUtils.FALSE)
public class LdapAuthenticationMapper implements ContextAuthenticationMapper {

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;
    @Override
    public AuthenticationResponse map(ConvertibleValues<Object> attributes, String username, Set<String> groups) {
        return new UserDetails(username, resourceBasedSecurityRule.computeRolesFromGroups(List.copyOf(groups)), Map.of("groups",groups));
    }
}
