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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcContext;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Optionals;

/**
 * WindGate adapter for JDBC operations.
 * @since 0.2.0
 */
public final class WindGateJdbcDirect {

    private WindGateJdbcDirect() {
        return;
    }

    /**
     * Builds a WindGate JDBC input.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param condition the condition expression (nullable)
     * @param jdbcAdapter the JDBC adapter
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcInputDriver> input(
            String profileName,
            String tableName,
            List<String> columnNames,
            String condition,
            ResultSetAdapter<?> jdbcAdapter,
            String... options) {
        Arguments.requireNonNull(options);
        return input(profileName, tableName, columnNames, condition, jdbcAdapter, Arguments.copyToSet(options));
    }

    /**
     * Builds a WindGate JDBC input.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param condition the condition expression (nullable)
     * @param jdbcAdapter the JDBC adapter
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcInputDriver> input(
            String profileName,
            String tableName,
            List<String> columnNames,
            String condition,
            ResultSetAdapter<?> jdbcAdapter,
            Set<String> options) {
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(tableName);
        Arguments.requireNonNull(columnNames);
        Arguments.requireNonNull(jdbcAdapter);
        Arguments.requireNonNull(options);
        return context -> {
            JdbcProfile profile = getProfile(context, profileName);
            Optional<String> cond = resolve(context, condition);
            String query = buildSelectStatement(profile, tableName, columnNames, cond, options);
            return new BasicJdbcInputDriver(profile, query, jdbcAdapter);
        };
    }

    /**
     * Builds a WindGate JDBC output with truncate operation.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param customTruncate the custom truncate statement
     * @param jdbcAdapter the JDBC adapter
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcOutputDriver> output(
            String profileName,
            String tableName,
            List<String> columnNames,
            String customTruncate,
            PreparedStatementAdapter<?> jdbcAdapter,
            String... options) {
        Arguments.requireNonNull(options);
        return output(profileName, tableName, columnNames, customTruncate, jdbcAdapter, Arguments.copyToSet(options));
    }

    /**
     * Builds a WindGate JDBC output with truncate operation.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param customTruncate the custom truncate statement (nullable)
     * @param jdbcAdapter the JDBC adapter
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcOutputDriver> output(
            String profileName,
            String tableName,
            List<String> columnNames,
            String customTruncate,
            PreparedStatementAdapter<?> jdbcAdapter,
            Set<String> options) {
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(tableName);
        Arguments.requireNonNull(columnNames);
        Arguments.requireNonNull(jdbcAdapter);
        Arguments.requireNonNull(options);
        return context -> {
            JdbcProfile profile = getProfile(context, profileName);
            JdbcOperationDriver truncate = new BasicJdbcOperationDriver(
                    profile,
                    resolve(context, customTruncate)
                        .orElseGet(() -> buildTruncateStatement(profile, tableName, columnNames, options)));
            String insert = buildInsertStatement(profile, tableName, columnNames, options);
            return new BasicJdbcOutputDriver(profile, truncate, insert, jdbcAdapter);
        };
    }

    /**
     * Builds a WindGate JDBC output with truncate operation.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param customTruncate the custom truncate statement (nullable)
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcOperationDriver> truncate(
            String profileName,
            String tableName,
            List<String> columnNames,
            String customTruncate,
            String... options) {
        Arguments.requireNonNull(options);
        return truncate(profileName, tableName, columnNames, customTruncate, Arguments.copyToSet(options));
    }

    /**
     * Builds a WindGate JDBC output with truncate operation.
     * @param profileName the profile name
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param customTruncate the custom truncate statement (nullable)
     * @param options the extra options
     * @return an input driver provider for the specs
     */
    public static Function<? super JdbcContext, ? extends JdbcOperationDriver> truncate(
            String profileName,
            String tableName,
            List<String> columnNames,
            String customTruncate,
            Set<String> options) {
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(tableName);
        Arguments.requireNonNull(columnNames);
        Arguments.requireNonNull(options);
        return context -> {
            JdbcProfile profile = getProfile(context, profileName);
            return new BasicJdbcOperationDriver(
                    profile,
                    resolve(context, customTruncate)
                        .orElseGet(() -> buildTruncateStatement(profile, tableName, columnNames, options)));
        };
    }

    private static JdbcProfile getProfile(JdbcContext context, String profileName) {
        return context.getEnvironment().getProfile(profileName);
    }

    private static Optional<String> resolve(JdbcContext context, String pattern) {
        return Optionals.of(pattern).map(context::resolve);
    }

    private static String buildSelectStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Optional<String> condition,
            Set<String> options) {
        return buildBasicSelectStatement(tableName, columnNames, condition);
    }

    private static String buildInsertStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Set<String> options) {
        // TODO optimize Oracle DIRPATH
        return buildBasicInsertStatement(tableName, columnNames);
    }

    private static String buildTruncateStatement(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            Set<String> options) {
        return buildBasicTruncateStatement(tableName);
    }

    private static String buildBasicSelectStatement(
            String tableName,
            List<String> columnNames,
            Optional<String> condition) {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ");
        buf.append(String.join(",", columnNames)); //$NON-NLS-1$
        buf.append(" FROM ");
        buf.append(tableName);
        condition.ifPresent(s -> buf.append(" WHERE ").append(s));
        return buf.toString();
    }

    private static String buildBasicTruncateStatement(String tableName) {
        StringBuilder buf = new StringBuilder();
        buf.append("TRUNCATE ");
        buf.append("TABLE ");
        buf.append(tableName);
        return buf.toString();
    }

    private static String buildBasicInsertStatement(String tableName, List<String> columnNames) {
        StringBuilder buf = new StringBuilder();
        buf.append("INSERT ");
        buf.append("INTO ");
        buf.append(tableName);
        buf.append(" (");
        buf.append(String.join(",", columnNames)); //$NON-NLS-1$
        buf.append(") ");
        buf.append("VALUES ");
        buf.append(" (");
        buf.append(String.join(",", Collections.nCopies(columnNames.size(), "?"))); //$NON-NLS-1$ //$NON-NLS-2$
        buf.append(")");
        return buf.toString();
    }
}
