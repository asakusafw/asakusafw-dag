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
package com.asakusafw.dag.compiler.jdbc.windgate;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.compiler.jdbc.JdbcDagCompilerTestRoot;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcTruncateProcessorGenerator.Spec;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironment;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;

/**
 * Test for {@link WindGateJdbcTruncateProcessorGenerator}.
 */
public class WindGateJdbcTruncateProcessorGeneratorTest extends JdbcDagCompilerTestRoot {

    private static final String PROFILE = "testing";

    private static final String TABLE = "KSV";

    private static final List<String> COLUMNS = Arrays.asList("M_KEY", "M_SORT", "M_VALUE");

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
        run(WindGateJdbcTruncateProcessorGenerator.generate(context(), new Spec(
                "x",
                PROFILE, TABLE, COLUMNS,
                null,
                Arrays.asList())));
        assertThat(select(), hasSize(0));
    }

    /**
     * w/ custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void custom() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        run(WindGateJdbcTruncateProcessorGenerator.generate(context(), new Spec(
                "x",
                PROFILE, TABLE, COLUMNS,
                "DELETE KSV WHERE M_KEY = 2",
                Arrays.asList())));
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }

    private void run(ClassData data) {
        add(data, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            try (JdbcEnvironment environment = environment(PROFILE)) {
                runner
                    .resource(StageInfo.class, STAGE)
                    .resource(JdbcEnvironment.class, environment)
                    .run();
            }
        });
    }
}
