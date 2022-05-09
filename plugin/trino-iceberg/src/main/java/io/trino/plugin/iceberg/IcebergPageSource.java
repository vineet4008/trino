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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.trino.plugin.iceberg.delete.TrinoRow;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.CloseableIterable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.util.Objects.requireNonNull;

public class IcebergPageSource
        implements UpdatablePageSource
{
    private final Type[] columnTypes;
    private final int[] expectedColumnIndexes;
    private final ConnectorPageSource delegate;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;
    private final Optional<DeleteFilter<TrinoRow>> deleteFilter;
    private final Supplier<IcebergPositionDeletePageSink> positionDeleteSinkSupplier;

    @Nullable
    private IcebergPositionDeletePageSink positionDeleteSink;

    public IcebergPageSource(
            List<IcebergColumnHandle> expectedColumns,
            List<IcebergColumnHandle> requiredColumns,
            ConnectorPageSource delegate,
            Optional<ReaderProjectionsAdapter> projectionsAdapter,
            Optional<DeleteFilter<TrinoRow>> deleteFilter,
            Supplier<IcebergPositionDeletePageSink> positionDeleteSinkSupplier)
    {
        // expectedColumns should contain columns which should be in the final Page
        // requiredColumns should include all expectedColumns as well as any columns needed by the DeleteFilter
        requireNonNull(expectedColumns, "expectedColumns is null");
        requireNonNull(requiredColumns, "requiredColumns is null");
        this.expectedColumnIndexes = new int[expectedColumns.size()];
        for (int i = 0; i < expectedColumns.size(); i++) {
            checkArgument(expectedColumns.get(i).equals(requiredColumns.get(i)), "Expected columns must be a prefix of required columns");
            expectedColumnIndexes[i] = i;
        }

        this.columnTypes = requiredColumns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");
        this.deleteFilter = requireNonNull(deleteFilter, "deleteFilter is null");
        this.positionDeleteSinkSupplier = requireNonNull(positionDeleteSinkSupplier, "positionDeleteSinkSupplier is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }
            if (dataPage == null) {
                return null;
            }

            if (deleteFilter.isPresent()) {
                int positionCount = dataPage.getPositionCount();
                int[] positionsToKeep = new int[positionCount];
                try (CloseableIterable<TrinoRow> filteredRows = deleteFilter.get().filter(CloseableIterable.withNoopClose(TrinoRow.fromPage(columnTypes, dataPage, positionCount)))) {
                    int positionsToKeepCount = 0;
                    for (TrinoRow rowToKeep : filteredRows) {
                        positionsToKeep[positionsToKeepCount] = rowToKeep.getPosition();
                        positionsToKeepCount++;
                    }
                    dataPage = dataPage.getPositions(positionsToKeep, 0, positionsToKeepCount).getColumns(expectedColumnIndexes);
                }
                catch (IOException e) {
                    throw new TrinoException(ICEBERG_BAD_DATA, "Failed to filter rows during merge-on-read operation", e);
                }
            }

            return dataPage;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, e);
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        if (positionDeleteSink == null) {
            positionDeleteSink = positionDeleteSinkSupplier.get();
        }
        positionDeleteSink.appendPage(new Page(rowIds));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (positionDeleteSink != null) {
            return positionDeleteSink.finish();
        }
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        if (positionDeleteSink != null) {
            positionDeleteSink.abort();
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = delegate.getMemoryUsage();
        if (positionDeleteSink != null) {
            memoryUsage += positionDeleteSink.getMemoryUsage();
        }
        return memoryUsage;
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        closeAllSuppress(throwable, this);
    }
}
