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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;

/**
 * Test for {@link WindGateJdbcDirect}.
 */
public class WindGateJdbcDirectTest extends JdbcDagTestRoot {

    private static final String TABLE = "KSV";

    private static final List<String> COLS = Arrays.asList("M_KEY", "M_SORT", "M_VALUE");

    /**
     * input - simple case.
     * @throws Exception if failed
     */
    @Test
    public void input() throws Exception {
        insert(0, null, "Hello, world!");
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(new KsvModel(0, null, "Hello, world!")));
        });
    }

    /**
     * input - multiple records.
     * @throws Exception if failed
     */
    @Test
    public void input_multiple() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3")));
        });
    }

    /**
     * input - w/ condition.
     * @throws Exception if failed
     */
    @Test
    public void input_conditional() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", Collections.singletonMap("V", "2"), c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .withCondition("M_KEY = ${V}")
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(new KsvModel(2, null, "Hello2")));
        });
    }

    /**
     * output - simple case.
     * @throws Exception if failed
     */
    @Test
    public void output() throws Exception {
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .build(c);
            put(driver, new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * output - multiple records.
     * @throws Exception if failed
     */
    @Test
    public void output_multiple() throws Exception {
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .build(c);
            put(driver,
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3"));
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * output - truncate all.
     * @throws Exception if failed
     */
    @Test
    public void output_truncate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .build(c);
            driver.initialize();
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * output - custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void output_truncate_custom() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", Collections.singletonMap("V", "2"), c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .withCustomTruncate("DELETE FROM KSV WHERE M_KEY = ${V}")
                    .build(c);
            driver.initialize();
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * output - w/ oracle dirpath.
     * @throws Exception if failed
     */
    @Test
    public void output_oracle_dirpath() throws Exception {
        options(WindGateJdbcDirect.OPTIMIAZATION_ORACLE_DIRPATH);
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLS, new KsvJdbcAdapter())
                    .withOptions(WindGateJdbcDirect.OPTIMIAZATION_ORACLE_DIRPATH)
                    .build(c);
            put(driver, new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * truncate - simple.
     * @throws Exception if failed
     */
    @Test
    public void truncate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLS)
                    .build(c);
            driver.perform();
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * output - custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void truncate_custom() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", Collections.singletonMap("V", "2"), c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLS)
                    .withCustomTruncate("DELETE FROM KSV WHERE M_KEY = ${V}")
                    .build(c);
            driver.perform();
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }
}
