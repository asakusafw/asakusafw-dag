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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link BufferOperatorGenerator}.
 */
@SuppressWarnings("deprecation")
public class BufferOperatorGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockResult<MockDataModel> m0 = new MockResult<>();
        MockResult<MockDataModel> m1 = new MockResult<>();
        check(Arrays.asList(m0, m1), r -> {
            MockDataModel o = new MockDataModel();
            o.getKeyOption().modify(1);
            r.add(o);
        });
        assertThat(m0.getResults(), hasSize(1));
        assertThat(m1.getResults(), hasSize(1));
        assertThat(m0.getResults().get(0).getKeyOption().get(), is(1));
        assertThat(m1.getResults().get(0).getKeyOption().get(), is(1));
    }

    /**
     * w/ edit sources.
     */
    @Test
    public void edit() {
        MockResult<MockDataModel> m0 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.getKeyOption().modify(result.getKeyOption().get() + 1);
                return result;
            }
        };
        MockResult<MockDataModel> m1 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.getKeyOption().modify(result.getKeyOption().get() + 2);
                return result;
            }
        };
        check(Arrays.asList(m0, m1), r -> {
            MockDataModel o = new MockDataModel();
            o.getKeyOption().modify(1);
            r.add(o);
        });
        assertThat(m0.getResults(), hasSize(1));
        assertThat(m1.getResults(), hasSize(1));
        assertThat(m0.getResults().get(0).getKeyOption().get(), is(2));
        assertThat(m1.getResults().get(0).getKeyOption().get(), is(3));
    }

    private void check(List<? extends Result<MockDataModel>> list, Consumer<Result<MockDataModel>> callback) {
        List<VertexElement> succs = new ArrayList<>();
        Class<?>[] parameterTypes = new Class<?>[list.size()];
        Object[] arguments = new Object[list.size()];
        for (int i = 0, n = list.size(); i < n; i++) {
            succs.add(new OutputNode("o" + i, typeOf(Result.class), typeOf(MockDataModel.class)));
            parameterTypes[i] = Result.class;
            arguments[i] = list.get(i);
        }
        ClassDescription generated = add(c -> new BufferOperatorGenerator().generate(succs, c));
        loading(generated, c -> {
            Constructor<?> ctor = c.getConstructor(list.stream().map(r -> Result.class).toArray(Class[]::new));
            @SuppressWarnings("unchecked")
            Result<MockDataModel> r = (Result<MockDataModel>) ctor.newInstance(list.stream().toArray());
            callback.accept(r);
        });
    }
}
