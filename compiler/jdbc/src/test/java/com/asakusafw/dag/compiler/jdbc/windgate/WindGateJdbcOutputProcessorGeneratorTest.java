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

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
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
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcOutputProcessorGenerator.Spec;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironment;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcOutputProcessor;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.utils.common.Tuple;
import com.asakusafw.lang.compiler.model.PropertyName;

/**
 * Test for {@link WindGateJdbcOutputProcessorGenerator}.
 */
public class WindGateJdbcOutputProcessorGeneratorTest extends JdbcDagCompilerTestRoot {

    private static final String PROFILE = "testing";

    private static final String TABLE = "KSV";

    private static final List<Tuple<String, PropertyName>> MAPPINGS =
            mappings("M_KEY:key", "M_SORT:sort", "M_VALUE:value");

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(999, null, "ERROR");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), new Spec(
                "x",
                typeOf(KsvModel.class),
                PROFILE, TABLE, MAPPINGS, null,
                Arrays.asList()));

        run(data, new KsvModel(0, null, "Hello, world!"));
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        insert(999, null, "ERROR");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), new Spec(
                "x",
                typeOf(KsvModel.class),
                PROFILE, TABLE, MAPPINGS, null,
                Arrays.asList()));
        run(data,
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3"));
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * w/ custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void truncate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), new Spec(
                "x",
                typeOf(KsvModel.class),
                PROFILE, TABLE, MAPPINGS,
                "DELETE KSV WHERE M_KEY != 2",
                Arrays.asList()));

        run(data, new KsvModel(0, null, "Hello, world!"));
        assertThat(select(), contains(
                new KsvModel(0, null, "Hello, world!"),
                new KsvModel(2, null, "Hello2")));
    }

    private void run(ClassData data, KsvModel... values) {
        add(data, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            runner
                .input(JdbcOutputProcessor.INPUT_NAME, (Object[]) values)
                .resource(StageInfo.class, STAGE)
                .resource(JdbcEnvironment.class, environment(PROFILE))
                .run();
        });
    }
}
