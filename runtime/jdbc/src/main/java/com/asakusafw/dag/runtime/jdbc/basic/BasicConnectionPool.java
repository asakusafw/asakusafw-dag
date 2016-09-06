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
package com.asakusafw.dag.runtime.jdbc.basic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Invariants;
import com.asakusafw.dag.utils.common.Optionals;

/**
 * A basic implementation of {@link ConnectionPool}.
 * @since 0.2.0
 */
public class BasicConnectionPool implements ConnectionPool {

    static final Logger LOG = LoggerFactory.getLogger(BasicConnectionPool.class);

    private final Driver driver;

    private final String url;

    private final Properties properties;

    private final int size;

    private final Semaphore semaphore;

    private final Queue<Connection> cached = new LinkedList<>();

    private boolean poolClosed = false;

    /**
     * Creates a new instance.
     * @param driver the JDBC driver instance (nullable)
     * @param url the JDBC URL
     * @param properties the JDBC properties
     * @param maxConnections the number of max connections
     */
    public BasicConnectionPool(Driver driver, String url, Map<String, String> properties, int maxConnections) {
        Arguments.requireNonNull(url);
        Arguments.requireNonNull(properties);
        Arguments.require(maxConnections >= 1);
        this.driver = driver;
        this.url = url;
        this.properties = new Properties();
        this.properties.putAll(properties);
        this.size = maxConnections;
        this.semaphore = new Semaphore(maxConnections);
    }

    /**
     * Creates a new instance.
     * @param url the JDBC URL
     * @param properties the JDBC properties
     * @param maxConnections the number of max connections
     */
    public BasicConnectionPool(String url, Map<String, String> properties, int maxConnections) {
        this(null, url, properties, maxConnections);
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public OptionalInt size() {
        return OptionalInt.of(size);
    }

    @Override
    public ConnectionPool.Handle acquire() throws IOException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("acquiring connection from pool: {}/{}", semaphore.availablePermits(), size); //$NON-NLS-1$
        }
        semaphore.acquire();
        boolean success = false;
        try {
            Connection connection = null;
            synchronized (cached) {
                if (poolClosed) {
                    throw new IOException("connection poll has been already closed");
                }
                while (cached.isEmpty() == false) {
                    connection = cached.poll();
                    if (connection.isClosed()) {
                        connection.close();
                        connection = null;
                    } else {
                        break;
                    }
                }
            }
            if (connection == null) {
                connection = acquire0();
            }
            connection.clearWarnings();
            connection.setAutoCommit(false);
            success = true;
            return new Handle(connection);
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            if (success == false) {
                semaphore.release();
            }
        }
    }

    private Connection acquire0() throws SQLException {
        LOG.debug("opening connection: {}", url); //$NON-NLS-1$
        if (driver != null) {
            return driver.connect(url, properties);
        } else {
            return DriverManager.getConnection(url, properties);
        }
    }

    void release(Connection connection) throws IOException {
        if (connection == null) {
            return;
        }
        try {
            if (connection.isClosed() == false && connection.getAutoCommit() == false) {
                connection.rollback();
            }
            synchronized (cached) {
                if (poolClosed) {
                    connection.close();
                } else {
                    cached.add(connection);
                }
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            semaphore.release();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (Closer closer = new Closer()) {
            synchronized (cached) {
                poolClosed = true;
                cached.forEach(c -> closer.add(JdbcUtil.wrap(c::close)));
                cached.clear();
            }
        }
    }

    /**
     * Provides {@link BasicConnectionPool} instance.
     * @since 0.2.0
     */
    public static class Provider implements ConnectionPool.Provider {

        @Override
        public ConnectionPool newInstance(String url, Map<String, String> properties, int maxConnections) {
            return new BasicConnectionPool(url, properties, maxConnections);
        }
    }

    private class Handle implements ConnectionPool.Handle {

        private final AtomicReference<Connection> connection;

        Handle(Connection connection) {
            Invariants.requireNonNull(connection);
            this.connection = new AtomicReference<>(connection);
        }

        @Override
        public Connection getConnection() throws IOException, InterruptedException {
            return Optionals.of(connection.get()).orElseThrow(IllegalStateException::new);
        }

        @Override
        public void close() throws IOException, InterruptedException {
            BasicConnectionPool.this.release(connection.getAndSet(null));
        }
    }
}
