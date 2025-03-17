/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.rest.auth;

import com.netflix.metatron.ipc.security.MetatronSslContext;
import com.netflix.metatron.ipc.security.MultiApplicationNameAuthVerifier;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.OAuth2Util.AuthSession;
import org.apache.iceberg.util.PropertyUtil;

public class NetflixAuthUtil {
    public static final String E2ETOKEN_HEADER = "X-Forwarded-Authentication";
    private static final String REST_METATRON_ENABLED = "rest.metatron-enabled";
    private static final String REST_E2ETOKEN_ENABLED = "rest.e2etoken-enabled";
    private static final String[] METATRON_ALLOWED_APPS = new String[]{"icebergrestcatalog", "nflxe2etokens"};
    private static SSLConnectionSocketFactory metatronSslFactory = new SSLConnectionSocketFactory(
            MetatronSslContext.forClient(new MultiApplicationNameAuthVerifier(METATRON_ALLOWED_APPS)),
            null,
            null,
            NoopHostnameVerifier.INSTANCE /* hostname verification is handled internally by MetatronSslContext */);

    public static void initMetatronSSLConnectionSocketFactory(
            PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder,
            Map<String, String> properties) {
        if (PropertyUtil.propertyAsBoolean(properties, REST_METATRON_ENABLED, false)) {
            connectionManagerBuilder.setSSLSocketFactory(metatronSslFactory);
        }
    }

    public static boolean isE2ETokenEnabled(Map<String, String> properties) {
        return PropertyUtil.propertyAsBoolean(properties, REST_E2ETOKEN_ENABLED,false);
    }

    public static boolean hasE2EToken(Map<String, String> properties) {
        return properties.containsKey(E2ETOKEN_HEADER);
    }

    public static AuthSession newAuthSessionWithE2E(Map<String, String> credentials, AuthSession parent) {
        return new AuthSession(
                RESTUtil.merge(credentials, Collections.singletonMap(E2ETOKEN_HEADER, credentials.get(E2ETOKEN_HEADER))),
                parent.token(),
                parent.tokenType(),
                parent.credential(),
                parent.scope());
    }

    public static SessionCatalog.SessionContext newSessionContextWithE2E(String e2eToken) {
        Map<String, String> credentials = Collections.emptyMap();
        if (e2eToken != null) {
            credentials = Collections.singletonMap(E2ETOKEN_HEADER, e2eToken);
        }
        return new SessionCatalog.SessionContext(
                UUID.randomUUID().toString(),
                null,
                credentials,
                Collections.emptyMap());
    }
}

