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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestIcebergV2
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private java.nio.file.Path tempDir;
    private File metastoreDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        tempDir = Files.createTempDirectory("test_iceberg_v2");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testSettingFormatVersion()
    {
        String tableName = "test_seting_format_version_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDefaultFormatVersion()
    {
        String tableName = "test_default_format_version_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertThat(loadTable(tableName).operations().current().formatVersion()).isEqualTo(2);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testV2TableRead()
    {
        String tableName = "test_v2_table_read" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        updateTableToV2(tableName);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testV2TableWithPositionDelete()
            throws Exception
    {
        String tableName = "test_v2_row_delete" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTableToV2(tableName);

        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileSystem fs = hdfsEnvironment.getFileSystem(new HdfsContext(SESSION), metadataDir);

        Path path = new Path(metadataDir, deleteFileName);
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(HadoopOutputFile.fromPath(path, fs))
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(icebergTable)
                .overwrite()
                .rowSchema(icebergTable.schema())
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        try (Closeable ignored = writer) {
            writer.delete(dataFilePath, 0, GenericRecord.create(icebergTable.schema()));
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 24");
    }

    @Test
    public void testV2TableWithEqualityDelete()
            throws Exception
    {
        String tableName = "test_v2_equality_delete" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTableToV2(tableName);
        writeEqualityDeleteToNationTable(icebergTable);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE regionkey != 1");
        assertQuery("SELECT nationkey FROM " + tableName, "SELECT nationkey FROM nation WHERE regionkey != 1");
    }

    @Test
    public void testUpgradeTableToV2FromTrino()
    {
        String tableName = "test_upgrade_table_to_v2_from_trino_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 1) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 1);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 2");
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
    }

    @Test
    public void testDowngradingV2TableToV1Fails()
    {
        String tableName = "test_downgrading_v2_table_to_v1_fails_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertThatThrownBy(() -> query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 1"))
                .hasMessage("Failed to commit new table properties")
                .getRootCause()
                .hasMessage("Cannot downgrade v2 table to v1");
    }

    @Test
    public void testUpgradingToInvalidVersionFails()
    {
        String tableName = "test_upgrading_to_invalid_version_fails_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format_version = 2) AS SELECT * FROM tpch.tiny.nation", 25);
        assertEquals(loadTable(tableName).operations().current().formatVersion(), 2);
        assertThatThrownBy(() -> query("ALTER TABLE " + tableName + " SET PROPERTIES format_version = 42"))
                .hasMessage("Unable to set catalog 'iceberg' table property 'format_version' to [42]: format_version must be between 1 and 2");
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable)
            throws Exception
    {
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + UUID.randomUUID();
        FileSystem fs = hdfsEnvironment.getFileSystem(new HdfsContext(SESSION), metadataDir);

        Schema deleteRowSchema = icebergTable.schema().select("regionkey");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(HadoopOutputFile.fromPath(new Path(metadataDir, deleteFileName), fs))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .equalityFieldIds(deleteRowSchema.findField("regionkey").fieldId())
                .overwrite()
                .buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.delete(dataDelete.copy("regionkey", 1L));
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private Table updateTableToV2(String tableName)
    {
        BaseTable table = loadTable(tableName);
        TableOperations operations = table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(2));

        return table;
    }

    private BaseTable loadTable(String tableName)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                CachingHiveMetastore.memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        return (BaseTable) loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));
    }
}
