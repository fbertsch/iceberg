package com.netflix.iceberg.security;

import com.netflix.bdp.security.authorization.Acl;
import com.netflix.bdp.security.authorization.AclJsonParser;
import com.netflix.bdp.security.authorization.AclStorage;
import com.netflix.bdp.security.authorization.AclUtils;
import com.netflix.bdp.security.authorization.MembershipChecker;
import com.netflix.bdp.security.authorization.Privilege;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import com.netflix.bdp.security.authorization.resource.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import static com.netflix.bdp.security.authorization.AclJsonParser.toJson;
import static com.netflix.bdp.security.authorization.AclStorage.extractResourceToAcls;
import static com.netflix.bdp.security.authorization.Privilege.ALL;

public class IcebergAclStorage implements AclStorage {

  private final Catalog catalog;
  private MembershipChecker membershipChecker = MembershipCheckerFactory.NO_OP_CHECKER;
  public static final String ACL_PROPERTY_KEY = "acls";

  public IcebergAclStorage(Catalog catalog) {
    this.catalog = catalog;
  }

  /**
   * With a provided membershipChecker, all names in ACLs will be mapped to ids before saved.
   * @param catalog
   * @param membershipChecker used to get the name to id mapping.
   */
  public IcebergAclStorage(Catalog catalog, MembershipChecker membershipChecker) {
    this.catalog = catalog;
    this.membershipChecker = membershipChecker;
  }

  @Override
  public Set<Acl> get(Resource resource) {
    Table table = catalog.loadTable(toTableIdentifier(resource));
    return getAcls(table);
  }

  private Set<Acl> getAcls(Table table) {
    Map<String, String> properties = table.properties();
    if (properties.containsKey(ACL_PROPERTY_KEY)) {
      String aclsJson = properties.get(ACL_PROPERTY_KEY);
      return getAcls(aclsJson);
    }
    return Collections.EMPTY_SET;
  }

  private Set<Acl> getAcls(String aclStr) {
    if (aclStr != null) {
      Set<Acl> acls = AclJsonParser.fromJson(aclStr);
      AclUtils.lowercaseResourceNames(acls);
      return acls;
    }
    return Collections.EMPTY_SET;
  }

  @Override
  public void add(Set<Acl> acls) {
    Map<Resource, Set<Acl>> resourceToAcls = extractResourceToAcls(acls);
    for (Entry<Resource, Set<Acl>> resourceAndAcls : resourceToAcls.entrySet()) {
      add(resourceAndAcls.getKey(), resourceAndAcls.getValue());
    }
  }

  private void add(Resource resource, Set<Acl> acls) {
    Table table = catalog.loadTable(toTableIdentifier(resource));
    transformAclProperty(table, existingAclsStr -> {
      Set<Acl> newAcls = Sets.newHashSet();
      newAcls.addAll(getAcls(existingAclsStr));
      newAcls.addAll(acls);
      newAcls = AclUtils.mapNameToId(newAcls, membershipChecker);
      AclUtils.lowercaseResourceNames(newAcls);
      return toJson(newAcls);
    });
  }

  private void transformAclProperty(Table table, Function<String, String> transformFunc) {
    UpdateProperties updateProperties = table.updateProperties();
    updateProperties.transform(ACL_PROPERTY_KEY, acl -> transformFunc.apply(acl));
    updateProperties.commit();
  }

  @Override
  public boolean remove(Set<Acl> acls) {
    Map<Resource, Set<Acl>> resourceToAcls = extractResourceToAcls(acls);
    boolean anyAclRemoved = false;
    for (Entry<Resource, Set<Acl>> resourceAndAcls : resourceToAcls.entrySet()) {
      anyAclRemoved = anyAclRemoved || remove(resourceAndAcls.getKey(), resourceAndAcls.getValue());
    }
    return anyAclRemoved;
  }

  private boolean remove(Resource resourceToRemove, Set<Acl> aclsToRemove) {
    Table table = catalog.loadTable(toTableIdentifier(resourceToRemove));
    AtomicBoolean anyAclRemoved = new AtomicBoolean(false);
    transformAclProperty(table, aclsStr -> removeAcls(aclsStr, resourceToRemove, aclsToRemove, anyAclRemoved));
    return anyAclRemoved.get();
  }

  private String removeAcls(String existingAclsStr, Resource resourceToRemove, Set<Acl> aclsToRemove, AtomicBoolean removed) {
    Set<Acl> existingAcls = getAcls(existingAclsStr);

    // Normalize both acls to use ids only
    existingAcls = AclUtils.mapNameToId(existingAcls, membershipChecker);
    AclUtils.lowercaseResourceNames(existingAcls);
    aclsToRemove = AclUtils.mapNameToId(aclsToRemove, membershipChecker);
    AclUtils.lowercaseResourceNames(aclsToRemove);

    Map<Resource, Set<Acl>> currentAclsMap = extractResourceToAcls(existingAcls);

    Set<Acl> newAcls = new HashSet<>();
    // TODO does revoke table level access means any column level access should also be removed?
    // for now we are only going to remove the table level access and would require that if any
    // column level access was explicitly added it must be removed explicitly too.
    Set<Acl> currentAcls = currentAclsMap.get(resourceToRemove);

    for (Acl aclToRemove : aclsToRemove) {
      // find matching acls with same principal and privilege as the acl that needs to be removed
      List<Acl> matchingCurrentAcls = new ArrayList<>();
      currentAcls.stream().forEach(acl -> {
        if (acl.principals().stream().anyMatch(aclToRemove.principals()::contains) &&
            (acl.privileges().stream().anyMatch(aclToRemove.privileges()::contains) || aclToRemove.privileges().contains(ALL))) {
          matchingCurrentAcls.add(acl);
        } else {
          newAcls.add(acl);
        }
      });

      for (Acl currentAcl : matchingCurrentAcls) {
        // if acls only partially match we got to preserve the parts that are not being removed,
        // otherwise we can just remove the entire matching acl as all parts are complete match.
        if (!aclToRemove.equals(currentAcl)) {
          // remove all principals that are part of acl to remove
          Set<NetflixPrincipal> principals = currentAcl.principals().stream()
              .filter(principal -> !aclToRemove.principals().contains(principal))
              .collect(Collectors.toSet());

          if(principals.size() != 0) {
            // for all other principals we want to keep the same privileges and resources so add the acl as is.
            newAcls.add(new Acl(principals, currentAcl.privileges(), currentAcl.resources(),
                currentAcl.grantor(), currentAcl.withGrant()));
          }

          Set<Resource> resources = new HashSet<>(currentAcl.resources());
          resources.removeAll(aclToRemove.resources());
          if(resources.size() != 0) {
            newAcls.add(new Acl(aclToRemove.principals(), currentAcl.privileges(), resources, currentAcl.grantor(), currentAcl.withGrant()));
          }

          if (!aclToRemove.privileges().contains(ALL)) {
            // if the privilege to remove is not ALL we add back set of privileges that are not removed.
            Set<Privilege> privileges = new HashSet<>(currentAcl.privileges());
            privileges.removeAll(aclToRemove.privileges());
            if (privileges.size() != 0) {
              newAcls.add(new Acl(aclToRemove.principals(), privileges, currentAcl.resources(),
                  currentAcl.grantor(), currentAcl.withGrant()));
            }
          }
        }
      }
    }
    removed.set(!newAcls.equals(currentAcls));
    return toJson(newAcls);
  }

  private TableIdentifier toTableIdentifier(Resource resource) {
    switch (resource.resourceType()) {
      case CATALOG:
        throw new IllegalArgumentException(
            "The catalog resource type does not have an iceberg equivalent table yet.");
      case SCHEMA:
        throw new IllegalArgumentException(
            "The schema resource type does not have an iceberg equivalent table yet.");
      case TABLE:
      case VIEW:
        String[] resourceNameParts = resourceNameParts(resource);
        return TableIdentifier.of(resourceNameParts);
      case COLUMN:
        resourceNameParts = resourceNameParts(resource);
        // remove the catalog and column name from parts list
        return TableIdentifier.of(Arrays.copyOfRange(resourceNameParts, 0, resourceNameParts.length - 1));
      default:
        throw new UnsupportedOperationException(
            "ResourceType: " + resource.resourceType() + "is not supported");
    }
  }

  private String[] resourceNameParts(Resource resource) {
    if (resource.parent() == null) {
      return new String[]{resource.resourceName()};
    } else {
      String[] resourceNameParts = resourceNameParts(resource.parent());
      return concat(resourceNameParts, new String[]{resource.resourceName()});
    }
  }

  public static String[] concat(String[] first, String[] second) {
    String[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }
}
