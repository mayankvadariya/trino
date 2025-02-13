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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.plugin.iceberg.IcebergSecurityConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergSecurityConfig.IcebergSecurity.READ_ONLY;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.aws.AwsProperties.REST_ACCESS_KEY_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_SECRET_ACCESS_KEY;
import static org.apache.iceberg.aws.AwsProperties.REST_SESSION_TOKEN;
import static org.apache.iceberg.aws.AwsProperties.REST_SIGNER_REGION;
import static org.apache.iceberg.aws.AwsProperties.REST_SIGNING_NAME;

public class SigV4AwsProperties
        implements AwsProperties
{
    private final Optional<StsAssumeRoleCredentialsProvider> iamRoleCredentialsProvider;
    private final Map<String, String> properties;

    private Optional<AwsSessionCredentials> currentAwsSessionCredentials = Optional.empty();
    private boolean hasIamRoleSessionCredentialsRefreshed;

    @Inject
    public SigV4AwsProperties(IcebergSecurityConfig securityConfig, IcebergRestCatalogSigV4Config sigV4Config, S3FileSystemConfig s3Config)
    {
        // TODO https://github.com/trinodb/trino/issues/24916 Allow write operations with SigV4
        checkArgument(securityConfig.getSecuritySystem() == READ_ONLY, "Read-only security system is required");

        properties = new HashMap<>();
        properties.put("rest.sigv4-enabled", "true");
        properties.put(REST_SIGNING_NAME, sigV4Config.getSigningName());
        properties.put(REST_SIGNER_REGION, requireNonNull(s3Config.getRegion(), "s3.region is null"));
        properties.put("rest-metrics-reporting-enabled", "false");

        if (s3Config.getIamRole() != null) {
            Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(s3Config);
            iamRoleCredentialsProvider = Optional.of(
                    StsAssumeRoleCredentialsProvider.builder()
                            .refreshRequest(request -> request
                                    .roleArn(s3Config.getIamRole())
                                    .roleSessionName("trino-iceberg-rest-catalog")
                                    .externalId(s3Config.getExternalId()))
                            .stsClient(createStsClient(s3Config, staticCredentialsProvider))
                            .asyncCredentialUpdateEnabled(true)
                            .build());
        }
        else {
            iamRoleCredentialsProvider = Optional.empty();
            properties.put(REST_ACCESS_KEY_ID, requireNonNull(s3Config.getAwsAccessKey(), "s3.aws-access-key is null"));
            properties.put(REST_SECRET_ACCESS_KEY, requireNonNull(s3Config.getAwsSecretKey(), "s3.aws-secret-key is null"));
        }
    }

    @Override
    public Map<String, String> get()
    {
        if (iamRoleCredentialsProvider.isPresent()) {
            AwsSessionCredentials newIamRoleSessionCredentials = (AwsSessionCredentials) iamRoleCredentialsProvider.get().resolveCredentials();
            if (currentAwsSessionCredentials.isEmpty() || !currentAwsSessionCredentials.get().equals(newIamRoleSessionCredentials)) {
                currentAwsSessionCredentials = Optional.of(newIamRoleSessionCredentials);
                hasIamRoleSessionCredentialsRefreshed = true;
            }
            else {
                hasIamRoleSessionCredentialsRefreshed = false;
            }
            properties.put(REST_ACCESS_KEY_ID, currentAwsSessionCredentials.get().accessKeyId());
            properties.put(REST_SECRET_ACCESS_KEY, currentAwsSessionCredentials.get().secretAccessKey());
            properties.put(REST_SESSION_TOKEN, currentAwsSessionCredentials.get().sessionToken());
        }
        return ImmutableMap.copyOf(properties);
    }

    @Override
    public boolean hasIamRoleSessionCredentialsRefreshed()
    {
        return hasIamRoleSessionCredentialsRefreshed;
    }

    private static Optional<AwsCredentialsProvider> createStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }

    private static StsClient createStsClient(S3FileSystemConfig config, Optional<AwsCredentialsProvider> credentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(config.getStsRegion())
                .or(() -> Optional.ofNullable(config.getRegion()))
                .map(Region::of).ifPresent(sts::region);
        credentialsProvider.ifPresent(sts::credentialsProvider);
        return sts.build();
    }
}
