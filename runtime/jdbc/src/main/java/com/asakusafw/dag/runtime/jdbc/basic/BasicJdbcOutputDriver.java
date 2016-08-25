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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.InterruptibleIo.Closer;

/**
 * A basic implementation of {@link JdbcOutputDriver}.
 * @since 0.2.0
 */
public class BasicJdbcOutputDriver implements JdbcOutputDriver {

    static final Logger LOG = LoggerFactory.getLogger(BasicJdbcOutputDriver.class);

    private static final int DEFAULT_INSERT_SIZE = 1024;

    private final JdbcProfile profile;

    private final JdbcOperationDriver initializer;

    private final String sql;

    private final PreparedStatementAdapter<?> adapter;

    /**
     * Creates a new instance.
     * @param profile the target JDBC profile
     * @param initializer the output initializer (nullable)
     * @param sql the insert statement with place-holders
     * @param adapter the prepared statement adapter
     */
    public BasicJdbcOutputDriver(
            JdbcProfile profile,
            JdbcOperationDriver initializer,
            String sql,
            PreparedStatementAdapter<?> adapter) {
        Arguments.requireNonNull(profile);
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapter);
        this.profile = profile;
        this.initializer = initializer;
        this.sql = sql;
        this.adapter = adapter;
    }

    @Override
    public int getMaxConcurrency() {
        return profile.getMaxOutputConcurrency()
                .orElseGet(JdbcOutputDriver.super::getMaxConcurrency);
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        if (initializer != null) {
            initializer.perform();
        }
    }

    @Override
    public ObjectWriter open() throws IOException, InterruptedException {
        int windowSize = profile.getInsertSize().orElse(DEFAULT_INSERT_SIZE);
        try (Closer closer = new Closer()) {
            ConnectionPool.Handle handle = closer.add(profile.acquire());
            PreparedStatement statement = handle.getConnection().prepareStatement(sql);
            closer.add(JdbcUtil.wrap(() -> statement.close()));
            return new BasicAppendCursor(statement, adapter, windowSize, closer.move());
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }
}
