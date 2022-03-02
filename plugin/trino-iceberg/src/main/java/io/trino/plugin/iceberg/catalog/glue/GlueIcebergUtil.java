package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.model.TableInput;

import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

public final class GlueIcebergUtil
{
    private GlueIcebergUtil() {}

    public static TableInput getTableInput(String tableName, Optional<String> owner, Map<String, String> parameters)
    {
        return new TableInput()
                .withName(tableName)
                .withOwner(owner.orElse(null))
                .withParameters(parameters)
                // Iceberg does not distinguish managed and external tables, all tables are treated the same and marked as EXTERNAL
                .withTableType(EXTERNAL_TABLE.name());
    }
}
