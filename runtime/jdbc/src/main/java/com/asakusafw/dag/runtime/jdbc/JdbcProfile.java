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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.util.OptionalInt;

import com.asakusafw.dag.utils.common.Arguments;

/**
 * Represents a JDBC target database profile.
 * @since 0.2.0
 */
public class JdbcProfile {

    private final String name;

    private final ConnectionPool connectionPool;

    /**
     * Creates a new instance.
     * @param name the profile name
     * @param connectionPool the connection pool for the profile
     */
    public JdbcProfile(String name, ConnectionPool connectionPool) {
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(connectionPool);
        this.name = name;
        this.connectionPool = connectionPool;
    }

    /**
     * Returns the profile name.
     * @return the profile name
     */
    public String getName() {
        return name;
    }

    /**
     * Acquires a JDBC connection handle.
     * @return the acquired connection handle
     * @throws IOException if I/O error was occurred while acquiring a connection
     * @throws InterruptedException if interrupted while acquiring a connection
     */
    public ConnectionPool.Handle acquire() throws IOException, InterruptedException {
        return connectionPool.acquire();
    }

    /**
     * Returns the input fetch size.
     * @return the input fetch size, or empty if it is not specified
     */
    public OptionalInt getFetchSize() {
        return OptionalInt.empty();
    }

    /**
     * Returns the input fetch size.
     * @return the input fetch size, or empty if it is not specified
     */
    public OptionalInt getInsertSize() {
        return OptionalInt.empty();
    }

    @Override
    public String toString() {
        return String.format("JdbcProfile(%s)", getName()); //$NON-NLS-1$
    }
}
