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
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.utils.common.Arguments;

/**
 * A basic implementation of {@link JdbcOperationDriver}.
 * @since 0.2.0
 */
public class BasicJdbcOperationDriver implements JdbcOperationDriver {

    static final Logger LOG = LoggerFactory.getLogger(BasicJdbcOperationDriver.class);

    private final JdbcProfile profile;

    private final String sql;

    /**
     * Creates a new instance.
     * @param profile the target JDBC profile
     * @param sql the delete statement
     */
    public BasicJdbcOperationDriver(JdbcProfile profile, String sql) {
        Arguments.requireNonNull(profile);
        Arguments.requireNonNull(sql);
        this.profile = profile;
        this.sql = sql;
    }

    @Override
    public void perform() throws IOException, InterruptedException {
        try (ConnectionPool.Handle handle = profile.acquire();
                Statement statement = handle.getConnection().createStatement()) {
            LOG.debug("execute: {}", sql);
            statement.execute(sql);
            LOG.debug("commit: {}", sql);
            handle.getConnection().commit();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }
}
