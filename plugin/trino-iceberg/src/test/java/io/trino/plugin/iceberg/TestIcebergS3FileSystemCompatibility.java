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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.RepairLegacyFileSystemTableProcedure.normalizeS3Uri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
@Testcontainers
public class TestIcebergS3FileSystemCompatibility
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_S3A_HADOOP_FS = "iceberg_s3_hadoop_fs";
    private static final String ICEBERG_S3A_HADOOP_FS2 = "iceberg_s3_hadoop_fs2";
    private static final String ICEBERG_S3A_NATIVE_FS = "iceberg_s3_native_fs";
    private static final String ACCESS_KEY = "accesskey";
    private static final String SECRET_KEY = "secretkey";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String SCHEMA_NAME = "test_schema";

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
            .withServices(LocalStackContainer.Service.S3);
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    private S3Client s3;

    @AfterAll
    public void tearDown()
    {
        if (s3 != null) {
            s3.close();
            s3 = null;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        s3 = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(LOCALSTACK.getRegion())).build();
        s3.createBucket(builder -> builder.bucket(BUCKET_NAME));

        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(ICEBERG_S3A_HADOOP_FS)
                        .setSchema(SCHEMA_NAME)
                        .build())
                .build();

        String catalogDir = "s3://%s/".formatted(BUCKET_NAME);
        Map<String, String> icebergS3HadoopFsProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                .put("hive.metastore.catalog.dir", catalogDir)
                .put("hive.s3.aws-access-key", ACCESS_KEY)
                .put("hive.s3.aws-secret-key", SECRET_KEY)
                .put("hive.s3.endpoint", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .put("hive.s3.region", Region.of(LOCALSTACK.getRegion()).toString())
                .put("fs.hadoop.enabled", "true")
                .put("fs.native-s3.enabled", "false")
                .buildOrThrow();

        Map<String, String> icebergS3HadoopFsProperties2 = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                .put("hive.metastore.catalog.dir", catalogDir)
                .put("hive.s3.aws-access-key", ACCESS_KEY)
                .put("hive.s3.aws-secret-key", SECRET_KEY)
                .put("hive.s3.endpoint", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .put("hive.s3.region", Region.of(LOCALSTACK.getRegion()).toString())
                .put("fs.hadoop.enabled", "true")
                .put("fs.native-s3.enabled", "false")
                .put("iceberg.repair-table-procedure.enabled", "true")
                .buildOrThrow();

        Map<String, String> icebergS3NativeFsProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                .put("hive.metastore.catalog.dir", catalogDir)
                .put("s3.aws-access-key", ACCESS_KEY)
                .put("s3.aws-secret-key", SECRET_KEY)
                .put("s3.endpoint", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString())
                .put("s3.region", Region.of(LOCALSTACK.getRegion()).toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .buildOrThrow();
        try {
            // Copied necessary code from IcebergQueryRunner to add multiple catalogs in queryRunner
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
            queryRunner.installPlugin(new TestingIcebergPlugin(dataDir));
            queryRunner.createCatalog(ICEBERG_S3A_HADOOP_FS, "iceberg", icebergS3HadoopFsProperties);
            queryRunner.createCatalog(ICEBERG_S3A_HADOOP_FS2, "iceberg", icebergS3HadoopFsProperties2);
            queryRunner.createCatalog(ICEBERG_S3A_NATIVE_FS, "iceberg", icebergS3NativeFsProperties);

            metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_S3A_HADOOP_FS)).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            fileSystemFactory = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_S3A_HADOOP_FS))
                    .getInjector().getInstance(TrinoFileSystemFactory.class);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testS3FileSystemCompatibility1()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS %s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME));
//        for (String s3Scheme : List.of("s3", "s3a", "s3n")) {
        for (String s3Scheme : List.of("s3a")) {
            testS3FileSystemCompatibility1(s3Scheme, true);
//            testS3FileSystemCompatibility(s3Scheme, false);
        }
    }

    private void testS3FileSystemCompatibility1(String s3Scheme, boolean isNonCanonicalPath)
    {
        String tableName = "sample_table" + randomNameSuffix();
        String dir1 = "dir1";
        String dir2 = "dir2";

        if (isNonCanonicalPath) {
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s WITH (location = '%s://%s/%s/%s//%s') AS SELECT 'test value' col".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName), 1);
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s//%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isTrue();
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
        }
        else {
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s WITH (location = '%s://%s/%s/%s/%s') AS SELECT 'test value' col".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName), 1);
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
        }

        assertUpdate("INSERT INTO %s.%s.%s values('test value2'), ('test value3'), ('test value4')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 3);
//        assertUpdate("INSERT INTO %s.%s.%s values('test value3')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
//        assertUpdate("INSERT INTO %s.%s.%s values('test value4')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("INSERT INTO %s.%s.%s values('test value5')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);

        assertUpdate("DELETE FROM %s.%s.%s WHERE col = 'test value4' OR col = 'test value3'".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 2);
//        assertUpdate("DELETE FROM %s.%s.%s WHERE col = 'test value3'".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);


        assertUpdate("ALTER TABLE %s.%s.%s ADD COLUMN int_col INTEGER".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));
        assertUpdate("UPDATE %s.%s.%s SET int_col = 1 WHERE col = 'test value'".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("UPDATE %s.%s.%s SET int_col = 2 WHERE col = 'test value2'".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("UPDATE %s.%s.%s SET int_col = 5 WHERE col = 'test value5'".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);

        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), "VALUES ('test value', 1), ('test value2', 2), ('test value5', 5)");

        // Query the same table in NativeFs
        assertQueryFails("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "Metadata not found in metadata location for table test_schema.%s".formatted(tableName));



//        assertUpdate("ALTER TABLE %s.%s.%s EXECUTE optimize(file_size_threshold => '128MB')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));

        // add col before

        // Migrate table from HadoopFs to NativeFs
//        assertUpdate("CALL iceberg.system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertUpdate("CALL %s.system.repair_table(schema_name => '%s', table_name => '%s', table_location => '%s://%s/%s/%s//%s')".formatted(ICEBERG_S3A_HADOOP_FS2, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName));
        //CALL iceberg.system.migrate_table_to_native_fs(schema_name => 'tpch', table_name => 'sample_table', table_location => 's3a://test-bucket/tpch/level1/level2/sample_table');
        // Query the same table in NativeFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "VALUES ('test value', 1), ('test value2', 2), ('test value5', 5)");
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), "VALUES ('test value', 1), ('test value2', 2), ('test value5', 5)");


        // add col after

        // queryable in hadoopfs
//        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "VALUES ('test value'), ('test value2'), ('test value5')"); // returns duplicate results



        //Check path in the all files before and after test
        assertUpdate("DROP TABLE %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));
    }


    @Test
    public void testTableWithPartition()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS %s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME));
//        for (String s3Scheme : List.of("s3", "s3a", "s3n")) {
        for (String s3Scheme : List.of("s3a")) {
            testTableWithPartition(s3Scheme, true);
//            testS3FileSystemCompatibility(s3Scheme, false);
        }
    }

    private void testTableWithPartition(String s3Scheme, boolean isNonCanonicalPath)
    {
        String tableName = "sample_table" + randomNameSuffix();;
        String dir1 = "dir1";
        String dir2 = "dir2";

        String tableLocation;
        if (isNonCanonicalPath) {
            tableLocation = "%s://%s/%s/%s//%s".formatted(s3Scheme, BUCKET_NAME, dir1, dir2, tableName);
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (format = 'PARQUET', partitioning = ARRAY['c1', 'c2'], sorted_by = ARRAY['c3'], location = '%s')"
                    .formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, tableLocation));
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s//%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isTrue();
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
        }
        else {
            tableLocation = "%s://%s/%s/%s/%s".formatted(s3Scheme, BUCKET_NAME, dir1, dir2, tableName);
            // Create table in HadoopFs
            assertUpdate("CREATE TABLE %s.%s.%s (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (format = 'PARQUET', partitioning = ARRAY['c1', 'c2'], sorted_by = ARRAY['c3'], location = '%s')"
                    .formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName, tableLocation), 1);
            // Verify that table is created at correct location
            assertThat(s3.listObjectsV2(builder -> builder.bucket(BUCKET_NAME).prefix("%s/%s/%s/".formatted(dir1, dir2, tableName))).contents().isEmpty()).isFalse();
        }

        assertUpdate("INSERT INTO %s.%s.%s values(0, DATE '2019-07-24', 1.7)".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("INSERT INTO %s.%s.%s values(1, DATE '2021-07-24', 5.3), (2, DATE '2022-05-20', 2.0), (3, DATE '2023-11-02', 3.4)".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 3);
        assertUpdate("INSERT INTO %s.%s.%s values(4, DATE '2023-12-05', 22.0)".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("INSERT INTO %s.%s.%s values(5, DATE '2024-01-02', 7.99)".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);

        assertUpdate("ALTER TABLE %s.%s.%s ADD COLUMN c4 varchar".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));
        assertUpdate("INSERT INTO %s.%s.%s values(6, DATE '2015-12-23', 83.32, 'value6')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);

        assertUpdate("DELETE FROM %s.%s.%s WHERE c1 = 0".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);
        assertUpdate("DELETE FROM %s.%s.%s WHERE c1 = 5".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), 1);

        // Query table in HadoopFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), "VALUES (1, DATE '2021-07-24', 5.3, NULL), (2, DATE '2022-05-20', 2.0, NULL), (3, DATE '2023-11-02', 3.4, NULL), (4, DATE '2023-12-05', 22.0, NULL), (6, DATE '2015-12-23', 83.32, 'value6')");

        // Query the same table in NativeFs
        assertQueryFails("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "Metadata not found in metadata location for table %s.%s".formatted(SCHEMA_NAME, tableName));

//        assertUpdate("ALTER TABLE %s.%s.%s EXECUTE optimize(file_size_threshold => '128MB')".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));

        verifyTableFiles(tableLocation, loadTable(tableName));
        // Migrate table from HadoopFs to NativeFs
        assertUpdate("CALL %s.system.repair_table(schema_name => '%s', table_name => '%s', table_location => '%s://%s/%s/%s//%s')".formatted(ICEBERG_S3A_HADOOP_FS2, SCHEMA_NAME, tableName, s3Scheme, BUCKET_NAME, dir1, dir2, tableName));

        // Query the same table in NativeFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_NATIVE_FS, SCHEMA_NAME, tableName), "VALUES (1, DATE '2021-07-24', 5.3, NULL), (2, DATE '2022-05-20', 2.0, NULL), (3, DATE '2023-11-02', 3.4, NULL), (4, DATE '2023-12-05', 22.0, NULL), (6, DATE '2015-12-23', 83.32, 'value6')");

        // queryable in HadoopFs
        assertQuery("SELECT * FROM %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName), "VALUES (1, DATE '2021-07-24', 5.3, NULL), (2, DATE '2022-05-20', 2.0, NULL), (3, DATE '2023-11-02', 3.4, NULL), (4, DATE '2023-12-05', 22.0, NULL), (6, DATE '2015-12-23', 83.32, 'value6')");

        verifyTableFiles(normalizeS3Uri(tableLocation), loadTable(tableName));
        //Check path in the all files before and after test
        assertUpdate("DROP TABLE %s.%s.%s".formatted(ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME, tableName));
    }

    private void verifyTableFiles(String expectedTableLocation, Table icebergTable)
    {
        assertThat(icebergTable.location()).isEqualTo(expectedTableLocation);
        assertThat(icebergTable.currentSnapshot().manifestListLocation()).startsWith(expectedTableLocation);
        assertThat(icebergTable.currentSnapshot().manifestListLocation()).startsWith(expectedTableLocation);

        icebergTable.currentSnapshot().allManifests(icebergTable.io()).forEach(manifestFile -> assertThat(manifestFile.path()).startsWith(expectedTableLocation));
        icebergTable.currentSnapshot().allManifests(icebergTable.io()).forEach(manifestFile -> assertThat(manifestFile.path()).startsWith(expectedTableLocation));
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, ICEBERG_S3A_HADOOP_FS, SCHEMA_NAME);
    }
}
