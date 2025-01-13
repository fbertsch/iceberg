/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.util.PropertyUtil;

public class NetflixE2ETokenHandler {
    private static final String E2ETOKEN_SERVICE_URL = "e2etoken.service-url";
    private static final String DEFAULT_E2ETOKEN_SERVICE_URL =
            "https://nflxe2etokens.us-east-1.prod.netflix.net/REST/v1/tokens/mint/metatron?targetApp=icebergrestcatalog";

    private static final String E2ETOKEN_REFRESH_LEAD_TIME_SEC = "e2etoken.refresh-lead-time-sec";
    private static final long DEFAULT_E2ETOKEN_REFRESH_LEAD_TIME_SEC = 300;

    private final String serviceUrl;
    private final long refreshLeadTimeSec;
    private volatile String token;
    private volatile long expiresAtSec;

    public NetflixE2ETokenHandler(Map<String, String> properties) {
        this.serviceUrl =
                PropertyUtil.propertyAsString(
                        properties, E2ETOKEN_SERVICE_URL, DEFAULT_E2ETOKEN_SERVICE_URL);
        this.refreshLeadTimeSec =
                PropertyUtil.propertyAsLong(
                        properties, E2ETOKEN_REFRESH_LEAD_TIME_SEC, DEFAULT_E2ETOKEN_REFRESH_LEAD_TIME_SEC);
    }

    public void addRequestHeaders(
            HttpUriRequest request, CloseableHttpClient httpClient, ObjectMapper mapper) {
        if (expiresAtSec < Instant.now().getEpochSecond() + refreshLeadTimeSec) {
            synchronized (this) {
                if (expiresAtSec < Instant.now().getEpochSecond() + refreshLeadTimeSec) {
                    refreshToken(httpClient, mapper);
                }
            }
        }
        request.setHeader("X-Forwarded-Authentication", token);
    }

    private void refreshToken(CloseableHttpClient httpClient, ObjectMapper mapper) {
        HttpGet tokenRequest = new HttpGet(serviceUrl);
        try (CloseableHttpResponse response = httpClient.execute(tokenRequest)) {
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JsonNode jsonNode = mapper.readTree(responseBody);
            this.token = jsonNode.get("token").asText();
            this.expiresAtSec = jsonNode.get("expiresAt").asLong();
        } catch (IOException | ParseException e) {
            throw new RESTException(e, "Error occurred while processing E2E token request");
        }
    }
}
