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

import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.client5.http.async.AsyncExecChainHandler;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.io.entity.HttpEntityWrapper;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Sig4HttpRequestInterceptor implements AsyncExecChainHandler {

    private final AwsCredentialsProvider credentialsProvider;

    private final String region;

    private final String serviceSigningName;

    public Sig4HttpRequestInterceptor(final AwsCredentialsProvider credentialsProvider, final String region,
            final String serviceSigningName) {
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.serviceSigningName = serviceSigningName;
    }

//    @Override
    public void process(HttpRequest request, EntityDetails entity, HttpContext context)
            throws HttpException, IOException {
        final var requestUri = uri(request);
        final var content = content(entity);
        AwsV4HttpSigner.create()
                .sign(r -> r.identity(credentialsProvider.resolveCredentials())
                        .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, region)
                        .putProperty(AwsV4HttpSigner.REGION_NAME, serviceSigningName)
                        .payload(ContentStreamProvider.fromInputStream(content))
                        .request(SdkHttpRequest.builder()
                                .uri(requestUri)
                                .method(SdkHttpMethod.fromValue(request.getMethod()))
                                .headers(headers(request))
                                .build()))
                .request()
                .headers()
                .entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream().map(v -> new BasicHeader(e.getKey(), v)))
                .forEach(request::setHeader);
    }

    private InputStream content(final EntityDetails entityDetails) throws IOException {
        if (entityDetails == null)
            return new ByteArrayInputStream(new byte[0]);

        if (entityDetails instanceof HttpEntity) {
            final var wrapper = new HttpEntityWrapper((HttpEntity) entityDetails);
            return wrapper.getContent();
        }
        return new ByteArrayInputStream(new byte[0]);
    }

    private Map<String, List<String>> headers(final HttpRequest request) {
        final var map = new HashMap<String, List<String>>();
        for (final var h : request.getHeaders()) {
            map.computeIfAbsent(h.getName(), s -> new LinkedList<>()).add(h.getValue());
        }
        return map;
    }

    private URI uri(final HttpRequest request) throws IOException {
        try {
            return request.getUri();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void execute(HttpRequest request, AsyncEntityProducer entityProducer, AsyncExecChain.Scope scope, AsyncExecChain chain, AsyncExecCallback asyncExecCallback) throws HttpException, IOException {
        System.err.println(request);
        chain.proceed(request, entityProducer, scope, asyncExecCallback);
    }
}
