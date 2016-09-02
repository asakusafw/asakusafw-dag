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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.InterruptibleIo.Closer;

/**
 * A basic implementation of {@link JdbcInputDriver}.
 * @since 0.2.0
 */
public class BasicJdbcInputDriver implements JdbcInputDriver {

    static final Logger LOG = LoggerFactory.getLogger(BasicJdbcInputDriver.class);

    private final JdbcProfile profile;

    private final String sql;

    private final Supplier<? extends ResultSetAdapter<?>> adapters;

    /**
     * Creates a new instance.
     * @param profile the target JDBC profile
     * @param sql the input query
     * @param adapters the result set adapter provider
     */
    public BasicJdbcInputDriver(
            JdbcProfile profile,
            String sql,
            Supplier<? extends ResultSetAdapter<?>> adapters) {
        Arguments.requireNonNull(profile);
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapters);
        this.profile = profile;
        this.sql = sql;
        this.adapters = adapters;
    }

    @Override
    public List<? extends Partition> getPartitions() {
        return Collections.singletonList(() -> open(profile, sql, adapters.get()));
    }

    /**
     * Creates a new reader which returns each object of query result.
     * @param profile the target JDBC profile
     * @param sql the input query
     * @param adapter the result set adapter
     * @return the created reader
     * @throws IOException if I/O error was occurred while computing input partitions
     * @throws InterruptedException if interrupted while computing input partitions
     */
    public static ObjectReader open(
            JdbcProfile profile,
            String sql,
            ResultSetAdapter<?> adapter) throws IOException, InterruptedException {
        Arguments.requireNonNull(profile);
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapter);
        LOG.debug("JDBC input ({}): {}", profile, sql);
        try (Closer closer = new Closer()) {
            Connection connection = closer.add(profile.acquire()).getConnection();
            Statement statement = connection.createStatement();
            closer.add(JdbcUtil.wrap(statement::close));
            if (profile.getFetchSize().isPresent()) {
                statement.setFetchSize(profile.getFetchSize().getAsInt());
            }
            ResultSet results = statement.executeQuery(sql);
            closer.add(JdbcUtil.wrap(results::close));

            return new BasicFetchCursor(results, adapter, closer.move());
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }
}
