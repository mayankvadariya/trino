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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class StaticMetastoreConfig
{
    public static final String HIVE_METASTORE_USERNAME = "hive.metastore.username";
    private List<URI> metastoreUris;
    private String metastoreUsername;

    @NotNull
    public List<URI> getMetastoreUris()
    {
        return metastoreUris;
    }

    @Config("hive.metastore.uri")
    @ConfigDescription("Hive metastore URIs (comma separated)")
    public StaticMetastoreConfig setMetastoreUris(List<String> uris)
    {
        if (uris == null) {
            this.metastoreUris = null;
            return this;
        }

        this.metastoreUris = uris.stream()
                .map(URI::create)
                .collect(toImmutableList());

        return this;
    }

    public String getMetastoreUsername()
    {
        return metastoreUsername;
    }

    @Config(HIVE_METASTORE_USERNAME)
    @ConfigDescription("Optional username for accessing the Hive metastore")
    public StaticMetastoreConfig setMetastoreUsername(String metastoreUsername)
    {
        this.metastoreUsername = metastoreUsername;
        return this;
    }

    @AssertTrue(message = "'hive.metastore.uri' must contain both http(s) and thrift URI schemes")
    public boolean isThriftScheme()
    {
        if (metastoreUris == null) {
            // metastoreUris is required, but that's validated on the getter
            return false;
        }

//        System.out.println("metastoreUris: ");
//        metastoreUris.forEach(System.out::println);
//        System.out.println("returned: " + metastoreUris.stream().anyMatch(uri -> "thrift".equalsIgnoreCase(uri.getScheme())));
//        var value = metastoreUris.stream().anyMatch(uri -> "thrift".equalsIgnoreCase(uri.getScheme()));
//        System.out.println("value is: " + value);
        return metastoreUris.stream().anyMatch(uri -> "thrift".equalsIgnoreCase(uri.getScheme()));
    }
}
