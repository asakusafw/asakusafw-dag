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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Rule;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.runtime.jdbc.basic.BasicConnectionPool;
import com.asakusafw.dag.runtime.jdbc.testing.H2Resource;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.utils.common.Action;
import com.asakusafw.dag.utils.common.InterruptibleIo.Closer;
import com.asakusafw.runtime.util.VariableTable;

/**
 * A common utilities for testing JDBC DAG.
 */
public abstract class JdbcDagTestRoot {

    /**
     * H2 resource.
     */
    @Rule
    public final H2Resource h2 = new H2Resource("cp")
        .with("CREATE TABLE KSV(M_KEY BIGINT, M_SORT DECIMAL(18,2), M_VALUE VARCHAR(256))");

    /**
     * Creates a new pool.
     * @param connections the max number of connections
     * @return the created connection pool
     */
    public BasicConnectionPool pool(int connections) {
        return new BasicConnectionPool(h2.getJdbcUrl(), Collections.emptyMap(), connections);
    }

    /**
     * Creates a new environment.
     * @param profileNames the profile names
     * @return the environment
     */
    public JdbcEnvironment environment(String... profileNames) {
        try (Closer closer = new Closer()) {
            List<JdbcProfile> profiles = new ArrayList<>();
            for (String name : profileNames) {
                profiles.add(new JdbcProfile(name, closer.add(pool(1))));
            }
            return new JdbcEnvironment(profiles, closer.move());
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Run an action with context.
     * @param profileName the profile name
     * @param action the action
     */
    public void context(String profileName, Action<JdbcContext, ?> action) {
        context(profileName, Collections.emptyMap(), action);
    }

    /**
     * Run an action with context.
     * @param profileName the profile name
     * @param variables the variables
     * @param action the action
     */
    public void context(String profileName, Map<String, String> variables, Action<JdbcContext, ?> action) {
        VariableTable vs = new VariableTable();
        vs.defineVariables(variables);
        try (JdbcEnvironment environment = environment(profileName)) {
            action.perform(new JdbcContext.Basic(environment, vs::parse));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Runs an action with profile.
     * @param profileName the profile name
     * @param action the action
     */
    public void profile(String profileName, Action<? super JdbcProfile, ?> action) {
        try (ConnectionPool pool = new BasicConnectionPool(h2.getJdbcUrl(), Collections.emptyMap(), 1)) {
            action.perform(new JdbcProfile(profileName, pool));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns all records from {@code KSV} table.
     * @return the results
     * @throws SQLException if error was occurred
     */
    public List<KsvModel> select() throws SQLException {
        try (Connection c = h2.open()) {
            return select(c);
        }
    }

    /**
     * Returns all records from {@code KSV} table.
     * @param connection the current connection
     * @return the results
     * @throws SQLException if error was occurred
     */
    public List<KsvModel> select(Connection connection) throws SQLException {
        String sql = "SELECT M_KEY, M_SORT, M_VALUE FROM KSV ORDER BY M_KEY, M_SORT";
        List<KsvModel> results = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                KsvModel record = new KsvModel();
                record.setKey(rs.getLong(1));
                record.setSort(rs.getBigDecimal(2));
                record.setValue(rs.getString(3));
                results.add(record);
            }
        }
        return results;
    }

    /**
     * Inserts a new record into {@code KSV} table.
     * @param key the key
     * @param sort the sort as string
     * @param value the value
     * @throws SQLException if error was occurred
     */
    public void insert(long key, String sort, String value) throws SQLException {
        try (Connection c = h2.open()) {
            insert(c, key, sort, value);
        }
    }

    /**
     * Inserts a new record into {@code KSV} table.
     * @param connection the current connection
     * @param key the key
     * @param sort the sort as string
     * @param value the value
     * @throws SQLException if error was occurred
     */
    public void insert(Connection connection, long key, String sort, String value) throws SQLException {
        insert(connection, key, sort == null ? null : new BigDecimal(sort), value);
    }

    private void insert(Connection connection, long key, BigDecimal sort, String value) throws SQLException {
        String sql = "INSERT INTO KSV(M_KEY, M_SORT, M_VALUE) VALUES(?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, key);
            statement.setBigDecimal(2, sort);
            statement.setString(3, value);
            statement.execute();
            connection.commit();
        }
    }

    /**
     * Returns values from the input driver.
     * @param driver the input driver
     * @return the obtained value
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public List<KsvModel> get(JdbcInputDriver driver) throws IOException, InterruptedException {
        List<KsvModel> results = new ArrayList<>();
        for (JdbcInputDriver.Partition partition : driver.getPartitions()) {
            try (ObjectReader reader = partition.open()) {
                while (reader.nextObject()) {
                    results.add(new KsvModel((KsvModel) reader.getObject()));
                }
            }
        }
        return results;
    }

    /**
     * Puts values into the output driver.
     * @param driver the output driver
     * @param values the values
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public void put(JdbcOutputDriver driver, Object... values) throws IOException, InterruptedException {
        try (ObjectWriter writer = driver.open()) {
            for (Object value : values) {
                writer.putObject(value);
            }
        }
    }
}
