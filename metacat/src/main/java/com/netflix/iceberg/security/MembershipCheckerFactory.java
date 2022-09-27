/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.security;

import com.netflix.bdp.security.authorization.MembershipChecker;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import com.netflix.bdp.security.gandalf.Client;
import com.netflix.bdp.security.gandalf.GandalfMembershipChecker;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MembershipCheckerFactory {
  private static final ConcurrentHashMap<String, MembershipChecker> CHECKERS = new ConcurrentHashMap<>();
  public static final MembershipChecker NO_OP_CHECKER = new MembershipChecker() {
    @Override
    public boolean isMember(NetflixPrincipal member, Set<NetflixPrincipal> group) {
      return false;
    }

    @Override
    public boolean isMember(NetflixPrincipal member, Set<NetflixPrincipal> group, Set<NetflixPrincipal> users) {
      return false;
    }

    @Override
    public Map<String, String> idToNameLookup(Set<String> userIds, Set<String> groupIds) {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, String> nameToIdLookup(Set<String> userNames, Set<String> groupNames) {
      return Collections.emptyMap();
    }
  };

  public static MembershipChecker getOrCreate(String bdpGandalfAgentHost, boolean isHttps) {
    return CHECKERS.computeIfAbsent(
        bdpGandalfAgentHost,
        host -> new GandalfMembershipChecker(new Client(host, isHttps)));
  }
}