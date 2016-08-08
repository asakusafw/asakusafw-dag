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
package com.asakusafw.dag.runtime.jdbc.util;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.utils.common.InterruptibleIo;
import com.asakusafw.dag.utils.common.RunnableWithException;

/**
 * Utilities about JDBC.
 * @since 0.2.0
 */
public final class JdbcUtil {

    static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    private JdbcUtil() {
        return;
    }

    /**
     * Returns an {@link IOException} object which wraps a {@link SQLException}.
     * @param exception the original exception
     * @return the wrapped exception
     */
    public static IOException wrap(SQLException exception) {
        int depth = 0;
        for (SQLException e = exception; e != null; e = e.getNextException()) {
            LOG.error("[{}]", depth++, e);
        }
        return new IOException(exception);
    }

    /**
     * Returns an {@link InterruptibleIo} operation which wraps an action with {@link SQLException}.
     * @param action the original action
     * @return the wrapped operation
     */
    public static InterruptibleIo wrap(RunnableWithException<SQLException> action) {
        return () -> {
            try {
                action.run();
            } catch (SQLException e) {
                throw wrap(e);
            }
        };
    }
}
