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
package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMetadata.commitUpdateAndTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

public class RepairLegacyFileSystemTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle MIGRATE_TABLE;

    private static final String PROCEDURE_NAME = "repair_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_LOCATION = "TABLE_LOCATION";
    private static final String METADATA_FILE_NAME = "METADATA_FILE_NAME";

    static {
        try {
            MIGRATE_TABLE = lookup().unreflect(RepairLegacyFileSystemTableProcedure.class.getMethod("migrateTable", ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean repairTableProcedureEnabled;

    @Inject
    public RepairLegacyFileSystemTableProcedure(TrinoCatalogFactory catalogFactory, TrinoFileSystemFactory fileSystemFactory, IcebergConfig icebergConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.repairTableProcedureEnabled = requireNonNull(icebergConfig, "icebergConfig is null").isRepairTableProcedureEnabled();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_LOCATION, VARCHAR),
                        new Procedure.Argument(METADATA_FILE_NAME, VARCHAR, false, null)),
                MIGRATE_TABLE.bindTo(this));
    }

    public void migrateTable(
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation,
            String metadataFileName)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation,
                    Optional.ofNullable(metadataFileName));
        }
    }

    private void doRegisterTable(
            ConnectorSession clientSession,
            String schemaName,
            String tableName,
            String tableLocation,
            Optional<String> metadataFileName)
    {
        if (!repairTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "repair_table procedure is disabled");
        }
        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(tableName != null && !tableName.isEmpty(), "table_name cannot be null or empty");
        checkProcedureArgument(tableLocation != null && !tableLocation.isEmpty(), "table_location cannot be null or empty");
        metadataFileName.ifPresent(RepairLegacyFileSystemTableProcedure::validateMetadataFileName);

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        TrinoCatalog catalog = catalogFactory.create(clientSession.getIdentity());
        if (!catalog.namespaceExists(clientSession, schemaTableName.getSchemaName())) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaTableName.getSchemaName()));
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(clientSession);
        String metadataLocation = getMetadataLocation(fileSystem, tableLocation, metadataFileName);
        validateMetadataLocation(fileSystem, Location.of(metadataLocation));
        TableMetadata tableMetadata;
        try {
            // Try to read the metadata file. Invalid metadata file will throw the exception.
            tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file: " + metadataLocation, e);
        }

        if (!locationEquivalent(tableLocation, tableMetadata.location())) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, """
                    Table metadata file [%s] declares table location as [%s] which is differs from location provided [%s]. \
                    Iceberg table can only be registered with the same location it was created with.""".formatted(metadataLocation, tableMetadata.location(), tableLocation));
        }

        Table table = catalog.loadTable(clientSession, schemaTableName);
        TableOperations operations = ((HasTableOperations) table).operations();
        FileIO fileIo = operations.io();

        List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests(fileIo);

        Transaction transaction = catalog.newTransaction(table);
        RewriteFiles rewriteFiles = transaction.newRewrite();

        manifestFiles.forEach(
                manifestFile -> {
                    String updatedManifestFilePath = normalizeS3Uri(manifestFile.path());

                    ManifestFile updatedManifestFile = new GenericManifestFile(
                            updatedManifestFilePath,
                            manifestFile.length(),
                            manifestFile.partitionSpecId(),
                            manifestFile.content(),
                            manifestFile.sequenceNumber(),
                            manifestFile.minSequenceNumber(),
                            manifestFile.snapshotId(),
                            manifestFile.addedFilesCount(),
                            manifestFile.addedRowsCount(),
                            manifestFile.existingFilesCount(),
                            manifestFile.existingRowsCount(),
                            manifestFile.deletedFilesCount(),
                            manifestFile.deletedRowsCount(),
                            manifestFile.partitions(),
                            manifestFile.keyMetadata());


                    List<PartitionSpec> partitionSpecs = table.specs().values().stream()
                            .filter(partitionSpec -> updatedManifestFile.partitionSpecId() == partitionSpec.specId())
                            .collect(toImmutableList());

                    if (manifestFile.content() == ManifestContent.DATA) {
//                        DataFile newDataFiles = ImmutableList.builder();
//                        DataFile newDataDeleteFiles = ImmutableList.builder(); // can't be empty otherwise something fails

                        ManifestReader<DataFile> reader = ManifestFiles.read(updatedManifestFile, operations.io());
                        reader.liveEntries().forEach(
                                file -> {
                                    DataFile dataFile = file.file();
                                    String updatedPath = normalizeS3Uri(dataFile.path().toString());
                                    DataFiles.Builder builder = DataFiles.builder(partitionSpecs.getFirst())
                                            .copy(dataFile)
                                            .withPath(updatedPath);
//                                    newDataFiles.add(builder.build());
//                                    newDataDeleteFiles.add(dataFile.copy());
                                    rewriteFiles.deleteFile(dataFile.copy());
                                    rewriteFiles.addFile(builder.build());
                                });

//                        rewriteFiles.rewriteFiles(new HashSet<>(newDataDeleteFiles.build()), new HashSet<>(newDataFiles.build()));
                    }
                    else if (manifestFile.content() == ManifestContent.DELETES) {
//                        ImmutableList.Builder<DeleteFile> newDeleteFiles = ImmutableList.builder();
//                        ImmutableList.Builder<DeleteFile> newDeleteDeleteFiles = ImmutableList.builder(); // can't be empty otherwise something fails

                        ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(updatedManifestFile, operations.io(), table.specs());
                        reader.entries().forEach(
                                file -> {
                                    DeleteFile deleteFile = file.file();
                                    String updatedPath = normalizeS3Uri(deleteFile.path().toString());
                                    DeleteFile updatedDeleteFile =
                                            new GenericDeleteFile(
                                                    deleteFile.specId(),
                                                    deleteFile.content(),
                                                    updatedPath,
                                                    deleteFile.format(),
                                                    (PartitionData) deleteFile.partition(),
                                                    deleteFile.fileSizeInBytes(),
                                                    new Metrics(deleteFile.recordCount(),
                                                            deleteFile.columnSizes(),
                                                            deleteFile.valueCounts(),
                                                            deleteFile.nullValueCounts(),
                                                            deleteFile.nanValueCounts(),
                                                            deleteFile.lowerBounds(),
                                                            deleteFile.upperBounds()),
                                                    deleteFile.equalityFieldIds() == null ? null : deleteFile.equalityFieldIds().stream().mapToInt(i -> i).toArray(),
                                                    deleteFile.sortOrderId(),
                                                    deleteFile.splitOffsets(),
                                                    deleteFile.keyMetadata()
                                            );
//                                    newDeleteFiles.add(updatedDeleteFile);
//                                    newDeleteDeleteFiles.add(deleteFile.copy());

                                    rewriteFiles.deleteFile(deleteFile.copy());
                                    rewriteFiles.addFile(updatedDeleteFile);
                                });

//                        rewriteFiles.rewriteFiles(new HashSet<>(newDeleteDeleteFiles.build()), new HashSet<>(newDeleteFiles.build()));
//                        rewriteFiles.rewriteFiles(new HashSet<>(newDeleteDeleteFiles.build()), new HashSet<>(newDeleteFiles.build()));
                    }
                }
        );
        commitUpdateAndTransaction(rewriteFiles, clientSession, transaction, "migrate_from_hadoop_fs_to_native_fs");

        //
        RewriteManifests rewriteManifests = transaction.rewriteManifests();
        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        manifests.forEach(
                manifestFile -> {
                    String updatedPath = normalizeS3Uri(manifestFile.path());
                    ManifestFile updatedFile;
                    if (manifestFile.content() == ManifestContent.DELETES) // delete file
                    {
                        updatedFile = new GenericManifestFile(
                                updatedPath,
                                manifestFile.length(),
                                manifestFile.partitionSpecId(),
                                manifestFile.content(),
                                -1, //
                                manifestFile.minSequenceNumber() - 1,
                                null, //manifestFile.snapshotId(),
                                0, //manifestFile.addedFilesCount(),
                                0, //manifestFile.addedRowsCount(),
                                manifestFile.existingFilesCount() + manifestFile.addedFilesCount(),// 1,
                                manifestFile.existingRowsCount() + manifestFile.addedRowsCount(), // TODO seems incorrect
                                0, //manifestFile.deletedFilesCount(),
                                0, //manifestFile.deletedRowsCount(),
                                manifestFile.partitions(),
                                manifestFile.keyMetadata());
                    }
                    else if (manifestFile.content() == ManifestContent.DATA) {
                        updatedFile = new GenericManifestFile(
                                updatedPath,
                                manifestFile.length(),
                                manifestFile.partitionSpecId(),
                                manifestFile.content(),
                                -1, //
                                manifestFile.minSequenceNumber() - 1,
                                null, //manifestFile.snapshotId(),
                                0, //manifestFile.addedFilesCount(),
                                0, //manifestFile.addedRowsCount(),
                                manifestFile.existingFilesCount() + manifestFile.addedFilesCount(), // added file count becomes existing count
                                manifestFile.existingRowsCount() + manifestFile.addedRowsCount(),
                                0, //manifestFile.deletedFilesCount(), // TODO Not sure if delete should be 0 or exiting count
                                0, //manifestFile.deletedRowsCount(),
                                manifestFile.partitions(),
                                manifestFile.keyMetadata());
                    }
                    else {
                        throw new IllegalStateException("should never happen");
                    }
                    rewriteManifests.addManifest(updatedFile);
                    rewriteManifests.deleteManifest(manifestFile);
                }
        );

        commitUpdateAndTransaction(rewriteManifests, clientSession, transaction, "migrate_from_hadoop_fs_to_native_fs");
    }

    private static void validateMetadataFileName(String fileName)
    {
        String metadataFileName = fileName.trim();
        checkProcedureArgument(!metadataFileName.isEmpty(), "metadata_file_name cannot be empty when provided as an argument");
        checkProcedureArgument(!metadataFileName.contains("/"), "%s is not a valid metadata file", metadataFileName);
        checkProcedureArgument(metadataFileName.startsWith("s3a://") || metadataFileName.startsWith("s3n://"), "Metadata file must have s3a/s3n scheme", metadataFileName);
    }

    /**
     * Get the latest metadata file location present in location if metadataFileName is not provided, otherwise
     * form the metadata file location using location and metadataFileName
     */
    private static String getMetadataLocation(TrinoFileSystem fileSystem, String location, Optional<String> metadataFileName)
    {
        return metadataFileName
                .map(fileName -> format("%s/%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME, fileName))
                .orElseGet(() -> getLatestMetadataLocation(fileSystem, location));
    }

    private static void validateMetadataLocation(TrinoFileSystem fileSystem, Location location)
    {
        try {
            if (!fileSystem.newInputFile(location).exists()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Metadata file does not exist: " + location);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Invalid metadata file location: " + location, e);
        }
    }

    private static boolean locationEquivalent(String a, String b)
    {
        return normalizeS3Uri(a).equals(normalizeS3Uri(b));
    }

    private static final Pattern SLASHES = Pattern.compile("/+");

    public static String normalizeS3Uri(String tableLocation)
    {
        // Normalize e.g. s3a to s3, so that table can be registered using s3:// location
        // even if internally it uses s3a:// paths.
        String normalizedSchema = tableLocation.replaceFirst("^s3[an]://", "s3://");
        URI uri = URI.create(normalizedSchema);
        String path = SLASHES.matcher(uri.getPath()).replaceAll("/");
        return "%s://%s%s".formatted(uri.getScheme(), uri.getHost(), path);
    }

}
