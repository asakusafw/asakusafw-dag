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
import com.asakusafw.dag.utils.common.Lang;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;
import com.asakusafw.vocabulary.operator.MasterCheck;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Test for {@link MasterCheckOperatorGenerator}.
 */
public class MasterCheckOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> f = new MockResult<>();
        MockResult<MockDataModel> m = new MockResult<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(f, m);
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyModel(0),
                    new MockKeyModel(0),
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "B"),
                },
            }));
        });
        assertThat(Lang.project(f.getResults(), e -> e.getValue()), contains("A", "B"));
        assertThat(Lang.project(m.getResults(), e -> e.getValue()), hasSize(0));
    }

    /**
     * missing masters.
     */
    @Test
    public void missing() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> f = new MockResult<>();
        MockResult<MockDataModel> m = new MockResult<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(f, m);
            r.add(cogroup(new Object[][] {
                {
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "B"),
                },
            }));
        });
        assertThat(Lang.project(f.getResults(), e -> e.getValue()), hasSize(0));
        assertThat(Lang.project(m.getResults(), e -> e.getValue()), contains("A", "B"));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selection() {
        UserOperator operator = load("selecting").build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> f = new MockResult<>();
        MockResult<MockDataModel> m = new MockResult<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(f, m);
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyModel(0),
                },
                {
                    new MockDataModel(0, "A"),
                    new MockDataModel(0, "B"),
                },
            }));
        });
        assertThat(Lang.project(f.getResults(), e -> e.getValue()), hasSize(0));
        assertThat(Lang.project(m.getResults(), e -> e.getValue()), contains("A", "B"));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(MasterCheck.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyModel.class))
                .input("transaction", Descriptions.typeOf(MockDataModel.class))
                .output("found", Descriptions.typeOf(MockDataModel.class))
                .output("missing", Descriptions.typeOf(MockDataModel.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @MasterCheck
        public void simple(MockKeyModel k, MockDataModel v) {
            return;
        }

        @MasterCheck(selection = "selector")
        public void selecting(MockKeyModel k, MockDataModel v) {
            return;
        }

        @MasterSelection
        public MockKeyModel selector(List<MockKeyModel> k, MockDataModel v) {
            return null;
        }
    }
}
