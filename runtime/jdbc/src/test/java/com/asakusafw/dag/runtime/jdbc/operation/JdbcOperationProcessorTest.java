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

import java.util.Collections;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.utils.common.Action;

/**
 * Test for {@link JdbcOperationProcessor}.
 */
public class JdbcOperationProcessorTest extends JdbcDagTestRoot {

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        profile("testing", p -> {
            run(c -> c.bind("t", new BasicJdbcOperationDriver(p, JdbcUtil.getDeleteStatement(TABLE, "M_KEY = 2"))));
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * w/ multiple operations.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        insert(4, null, "Hello4");
        insert(5, null, "Hello5");
        profile("testing", p -> {
            run(c -> c
                    .bind("t1", new BasicJdbcOperationDriver(p, JdbcUtil.getDeleteStatement(TABLE, "M_KEY = 1")))
                    .bind("t2", new BasicJdbcOperationDriver(p, JdbcUtil.getDeleteStatement(TABLE, "M_KEY = 3")))
                    .bind("t3", new BasicJdbcOperationDriver(p, JdbcUtil.getDeleteStatement(TABLE, "M_KEY = 5"))));
        });
        assertThat(select(), contains(
                new KsvModel(2, null, "Hello2"),
                new KsvModel(4, null, "Hello4")));
    }

    private void run(Action<JdbcOperationProcessor, Exception> config) {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> {
            JdbcOperationProcessor proc = new JdbcOperationProcessor();
            config.perform(proc);
            return proc;
        });
        runner
            .resource(StageInfo.class, STAGE)
            .resource(JdbcEnvironment.class, environment())
            .run();
    }
}
