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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;

/**
 * Test for {@link BasicJdbcOutputDriver}.
 */
public class BasicJdbcOutputDriverTest extends JdbcDagTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        profile("testing", p -> {
            put(driver(p), new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        profile("testing", p -> {
            put(driver(p),
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
     * truncate.
     * @throws Exception if failed
     */
    @Test
    public void truncate() throws Exception {
        profile("testing", p -> {
            BasicJdbcOutputDriver driver = driver(p);
            put(driver, new KsvModel(0, null, "Hello, world!"));
            driver.initialize();
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * w/ options.
     * @throws Exception if failed
     */
    @Test
    public void options() throws Exception {
        edit(b -> b.withMaxOutputConcurrency(123)
                .withOption(JdbcOutputDriver.Granularity.class, JdbcOutputDriver.Granularity.FINE));
        profile("testing", p -> {
            BasicJdbcOutputDriver driver = driver(p);
            assertThat(driver.getMaxConcurrency(), is(123));
            assertThat(driver.getGranularity(), is(JdbcOutputDriver.Granularity.FINE));
        });
    }

    private BasicJdbcOutputDriver driver(JdbcProfile profile) {
        return new BasicJdbcOutputDriver(
                profile,
                new BasicJdbcOperationDriver(profile, JdbcUtil.getTruncateStatement(TABLE)),
                JdbcUtil.getInsertStatement(TABLE, COLUMNS),
                KsvJdbcAdapter::new);
    }
}
