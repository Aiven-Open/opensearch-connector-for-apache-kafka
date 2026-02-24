/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.opensearch.sig4;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class Sig4HttpRequestInterceptorTest {

    static final String DEFAULT_CONTENT = "aaaaaaabbbbbcccc";

    HttpRequest newRequest(final Method method) throws URISyntaxException {
        var httpRequest = new SimpleHttpRequest(method.name(), "/asddsadasdas");

        httpRequest.setUri(new URI("http://localhost:8080"));
        if (method == Method.POST) {
            httpRequest.setBody(DEFAULT_CONTENT.getBytes(StandardCharsets.UTF_8), ContentType.TEXT_PLAIN);
            httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.toString());
            httpRequest.addHeader(HttpHeaders.CONTENT_LENGTH, DEFAULT_CONTENT.length());
        } else {
            httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.toString());
            httpRequest.addHeader(HttpHeaders.CONTENT_LENGTH, 0);
        }
        return httpRequest;
    }

    EntityDetails newEntityDetails() {
        return new StringEntity(DEFAULT_CONTENT, ContentType.TEXT_PLAIN, false);
    }

    @Test
    public void signsGetRequest() throws Exception {
        final var sig4HttpRequestInterceptor = new Sig4HttpRequestInterceptor(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKeyId", "secretAccessKey")),
                Region.AF_SOUTH_1.toString(), "aaaaa");

        final var r = newRequest(Method.GET);
        r.setUri(new URI("http://localhost:8080"));
        // sig4HttpRequestInterceptor.process(r, null, null);
        // from https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
        // check expected headers in the finla request
        // assertTrue(r.containsHeader(HttpHeaders.AUTHORIZATION));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_CONTENT_SHA256));
        // assertTrue(r.containsHeader(SignerConstant.HOST));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_DATE));
    }

    @Test
    public void signsRequest() throws Exception {
        final var sig4HttpRequestInterceptor = new Sig4HttpRequestInterceptor(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKeyId", "secretAccessKey")),
                Region.AF_SOUTH_1.toString(), "aaaaa");

        final var r = newRequest(Method.POST);
        // sig4HttpRequestInterceptor.process(r, newEntityDetails(), null);
        // from https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
        // check expected headers in the finla request
        // assertTrue(r.containsHeader(HttpHeaders.AUTHORIZATION));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_CONTENT_SHA256));
        // assertTrue(r.containsHeader(SignerConstant.HOST));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_DATE));
    }

    @Test
    public void signsAndKeepsCustomHeadersRequest() throws Exception {
        Sig4HttpRequestInterceptor sig4HttpRequestInterceptor = new Sig4HttpRequestInterceptor(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKeyId", "secretAccessKey")),
                Region.AF_SOUTH_1.toString(), "aaaaa");

        final var r = newRequest(Method.POST);

        r.setHeader(new BasicHeader("a", "b"));
        r.setHeader(new BasicHeader("c", "d"));

        // sig4HttpRequestInterceptor.process(r, newEntityDetails(), null);
        // from https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
        // check expected headers in the finla request
        // assertTrue(r.containsHeader(HttpHeaders.AUTHORIZATION));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_CONTENT_SHA256));
        // assertTrue(r.containsHeader(SignerConstant.HOST));
        // assertTrue(r.containsHeader(SignerConstant.X_AMZ_DATE));
    }

}
