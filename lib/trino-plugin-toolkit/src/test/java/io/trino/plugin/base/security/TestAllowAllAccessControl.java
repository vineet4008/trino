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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestAllowAllAccessControl
{
    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, AllowAllAccessControl.class, ImmutableSet.of(
                AllowAllAccessControl.class.getMethod("getRowFilter", ConnectorSecurityContext.class, SchemaTableName.class),
                AllowAllAccessControl.class.getMethod("getColumnMask", ConnectorSecurityContext.class, SchemaTableName.class, String.class, Type.class)));
    }
}
