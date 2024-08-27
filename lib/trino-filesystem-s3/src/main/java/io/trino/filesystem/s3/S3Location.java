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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;

import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record S3Location(Location location)
{
    private static final Pattern SLASHES = Pattern.compile("/+");

    S3Location
    {
        checkArgument(location.scheme().isPresent(), "No scheme for S3 location: %s", location);
        location = requireNonNull(getHadoopFsCompatibleLocation(location), "location is null");
        checkArgument(Set.of("s3", "s3a", "s3n").contains(location.scheme().get()), "Wrong scheme for S3 location: %s", location);
        checkArgument(location.host().isPresent(), "No bucket for S3 location: %s", location);
        checkArgument(location.userInfo().isEmpty(), "S3 location contains user info: %s", location);
        checkArgument(location.port().isEmpty(), "S3 location contains port: %s", location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String bucket()
    {
        return location.host().orElseThrow();
    }

    public String key()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s/".formatted(scheme(), bucket()));
    }

    // Required as HadoopFs normalizes path for s3a/s3n scheme but revives path for s3 schemes https://github.com/trinodb/trino/blob/0c5b0cf613e631fc59758acfeb813596b96facfe/lib/trino-hdfs/src/main/java/io/trino/filesystem/hdfs/HadoopPaths.java#L27-L36
    private static Location getHadoopFsCompatibleLocation(Location location)
    {
        String path = location.path().replaceAll("^/+", ""); // remove leading slashes
        if ("s3a".equals(location.scheme().orElseThrow()) || "s3n".equals(location.scheme().orElseThrow())) {
            path = SLASHES.matcher(path).replaceAll("/");
        }
        return Location.of("%s://%s/%s".formatted(location.scheme().orElseThrow(), location.host().orElseThrow(), path));
    }
}
