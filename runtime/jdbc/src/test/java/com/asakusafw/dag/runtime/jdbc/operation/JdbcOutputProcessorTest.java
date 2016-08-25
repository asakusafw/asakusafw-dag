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
package com.asakusafw.dag.runtime.jdbc.operation;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.utils.common.Action;

/**
 * Test for {@link JdbcOutputProcessor}.
 */
public class JdbcOutputProcessorTest extends JdbcDagTestRoot {

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(999, null, "ERROR");
        profile("testing", profile -> {
            run(c -> c.bind("t", driver(profile)),
                    new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        insert(999, null, "ERROR");
        profile("testing", profile -> {
            run(c -> c.bind("t", driver(profile)),
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3"));
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    private JdbcOutputDriver driver(JdbcProfile profile) {
        return new BasicJdbcOutputDriver(
                profile,
                new BasicJdbcOperationDriver(profile, "TRUNCATE TABLE KSV"),
                "INSERT INTO KSV (M_KEY, M_SORT, M_VALUE) VALUES (?, ?, ?)",
                new KsvJdbcAdapter());
    }

    private void run(
            Action<JdbcOutputProcessor, Exception> config,
            KsvModel... values) throws IOException, InterruptedException {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> {
            JdbcOutputProcessor proc = new JdbcOutputProcessor();
            config.perform(proc);
            return proc;
        });
        try (JdbcEnvironment environment = environment()) {
            runner
                .input(JdbcOutputProcessor.INPUT_NAME, (Object[]) values)
                .resource(StageInfo.class, STAGE)
                .resource(JdbcEnvironment.class, environment)
                .run();
        }
    }
}
