/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
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
package com.asakusafw.dag.compiler.jdbc.windgate;

import java.util.List;
import java.util.Set;

import com.asakusafw.dag.utils.common.Tuple;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents a WindGate JDBC input model.
 * @since 0.2.0
 */
public class WindGateJdbcOutputModel extends WindGateJdbcModel {

    private final String customTruncate;

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param profileName the profile name
     * @param tableName the table name
     * @param columnMappings the column mappings
     * @param customTruncate the custom truncate statement (nullable)
     * @param options the WindGate options
     */
    public WindGateJdbcOutputModel(
            TypeDescription dataType,
            String profileName,
            String tableName,
            List<Tuple<String, PropertyName>> columnMappings,
            String customTruncate,
            Set<String> options) {
        super(dataType, profileName, tableName, columnMappings, options);
        this.customTruncate = customTruncate;
    }

    /**
     * Returns the custom truncate statement.
     * @return the custom truncate statement, or {@code null} if it is not defined
     */
    public String getCustomTruncate() {
        return customTruncate;
    }
}
