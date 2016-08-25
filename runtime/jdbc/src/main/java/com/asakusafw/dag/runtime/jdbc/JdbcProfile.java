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

    private int fetchSize;

    private int insertSize;

    private int maxInputConcurrency;

    private int maxOutputConcurrency;

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
     * Creates a new instance.
     * @param name the profile name
     * @param connectionPool the connection pool for the profile
     * @param fetchSize the number of bulk fetch records, or {@code <= 0} if it is not defined
     * @param insertSize the number of bulk insert records, or {@code <= 0} if it is not defined
     * @param maxInputConcurrency the number of threads per input operation, or {@code <= 0} if it is not defined
     * @param maxOutputConcurrency the number of threads per output operation, or {@code <= 0} if it is not defined
     */
    public JdbcProfile(
            String name, ConnectionPool connectionPool,
            int fetchSize, int insertSize,
            int maxInputConcurrency, int maxOutputConcurrency) {
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(connectionPool);
        this.name = name;
        this.connectionPool = connectionPool;
        this.fetchSize = fetchSize;
        this.insertSize = insertSize;
        this.maxInputConcurrency = maxInputConcurrency;
        this.maxOutputConcurrency = maxOutputConcurrency;
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
        return getOptionalSize(fetchSize);
    }

    /**
     * Returns the input fetch size.
     * @return the input fetch size, or empty if it is not specified
     */
    public OptionalInt getInsertSize() {
        return getOptionalSize(insertSize);
    }

    /**
     * Returns the max number of threads for individual input operations.
     * @return max input concurrency, or empty if it is not specified
     */
    public OptionalInt getMaxInputConcurrency() {
        return getOptionalSize(maxInputConcurrency);
    }

    /**
     * Returns the max number of threads for individual output operations.
     * @return max output concurrency, or empty if it is not specified
     */
    public OptionalInt getMaxOutputConcurrency() {
        return getOptionalSize(maxOutputConcurrency);
    }

    private static OptionalInt getOptionalSize(int size) {
        return size <= 0 ? OptionalInt.empty() : OptionalInt.of(size);
    }

    @Override
    public String toString() {
        return String.format("JdbcProfile(%s)", getName()); //$NON-NLS-1$
    }
}
