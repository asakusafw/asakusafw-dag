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
package com.asakusafw.dag.compiler.builtin;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Test for {@link MasterBranchOperatorGenerator}.
 */
public class MasterBranchOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> a = new MockSink<>();
        MockSink<MockDataModel> b = new MockSink<>();
        MockSink<MockDataModel> c = new MockSink<>();
        MockSink<MockDataModel> x = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(a, b, c, x);
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyModel(0),
                    new MockKeyModel(0),
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "C"),
                },
            }));
        });
        assertThat(a.get(MockDataModel::getValue), contains("A"));
        assertThat(b.get(MockDataModel::getValue), hasSize(0));
        assertThat(c.get(MockDataModel::getValue), contains("C"));
        assertThat(x.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * missing masters.
     */
    @Test
    public void missing() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> a = new MockSink<>();
        MockSink<MockDataModel> b = new MockSink<>();
        MockSink<MockDataModel> c = new MockSink<>();
        MockSink<MockDataModel> x = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(a, b, c, x);
            r.add(cogroup(new Object[][] {
                {
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "C"),
                },
            }));
        });
        assertThat(a.get(MockDataModel::getValue), hasSize(0));
        assertThat(b.get(MockDataModel::getValue), hasSize(0));
        assertThat(c.get(MockDataModel::getValue), hasSize(0));
        assertThat(x.get(MockDataModel::getValue), containsInAnyOrder("A", "C"));
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized() {
        UserOperator operator = load("parameterized")
                .argument("p", Descriptions.valueOf("?"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> a = new MockSink<>();
        MockSink<MockDataModel> b = new MockSink<>();
        MockSink<MockDataModel> c = new MockSink<>();
        MockSink<MockDataModel> x = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(a, b, c, x, "B");
            r.add(cogroup(new Object[][] {
                {
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "C"),
                },
            }));
        });
        assertThat(a.get(MockDataModel::getValue), hasSize(0));
        assertThat(b.get(MockDataModel::getValue), containsInAnyOrder("A", "C"));
        assertThat(c.get(MockDataModel::getValue), hasSize(0));
        assertThat(x.get(MockDataModel::getValue), hasSize(0));
    }

    private Builder load(String name) {
        Builder builder = OperatorExtractor.extract(MasterBranch.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyModel.class))
                .input("transaction", Descriptions.typeOf(MockDataModel.class));
        for (Switch s : Switch.values()) {
            builder.output(PropertyName.of(s.name()).toMemberName(), Descriptions.typeOf(MockDataModel.class));
        }
        return builder;
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @MasterBranch
        public Switch simple(MockKeyModel k, MockDataModel v) {
            return parameterized(k, v, Switch.X.name());
        }

        @MasterBranch
        public Switch parameterized(MockKeyModel k, MockDataModel v, String defaultName) {
            if (k == null) {
                return Switch.valueOf(defaultName);
            }
            return Switch.valueOf(v.getValue());
        }

        @MasterBranch(selection = "selector")
        public Switch selecting(MockKeyModel k, MockDataModel v) {
            return simple(k, v);
        }

        @MasterSelection
        public MockKeyModel selector(List<MockKeyModel> k, MockDataModel v) {
            return null;
        }
    }

    @SuppressWarnings("javadoc")
    public enum Switch {
        A, B, C, X,
    }
}
