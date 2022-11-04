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

import com.netflix.bdp.security.authentication.PrincipalExtractor;
import com.netflix.bdp.security.authentication.RequestIdentity;
import com.netflix.bdp.security.authorization.principal.NetflixPrincipal;
import com.netflix.bdp.security.gandalf.Client;
import com.netflix.bdp.security.gandalf.GandalfPrincipalExtractor;
import java.util.concurrent.ConcurrentHashMap;

public class PrincipalExtractorFactory {
  private static final ConcurrentHashMap<String, PrincipalExtractor> EXTRACTORS = new ConcurrentHashMap<>();
  public static final PrincipalExtractor NO_OP_EXTRACTOR = new PrincipalExtractor() {
    @Override
    public NetflixPrincipal getPrincipal(RequestIdentity identity) {
      return null;
    }
  };

  public static PrincipalExtractor getOrCreate(String bdpGandalfAgentHost, boolean isHttps) {
    return EXTRACTORS.computeIfAbsent(
        bdpGandalfAgentHost,
        host -> new GandalfPrincipalExtractor(new Client(host, isHttps)));
  }
}
