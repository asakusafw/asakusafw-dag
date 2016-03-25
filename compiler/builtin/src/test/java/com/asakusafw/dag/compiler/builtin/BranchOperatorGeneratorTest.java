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

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.dag.runtime.testing.MockValueModel;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Branch;

/**
 * Test for {@link BranchOperatorGenerator}.
 */
public class BranchOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockValueModel> a = new MockSink<>();
        MockSink<MockValueModel> b = new MockSink<>();
        MockSink<MockValueModel> c = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(a, b, c);
            r.add(new MockValueModel("A"));
            r.add(new MockValueModel("C"));
        });
        assertThat(a.get(MockValueModel::getValue), contains("A"));
        assertThat(b.get(MockValueModel::getValue), hasSize(0));
        assertThat(c.get(MockValueModel::getValue), contains("C"));
    }

    private Builder load(String name) {
        ReifiableTypeDescription type = Descriptions.typeOf(MockValueModel.class);
        Builder builder = OperatorExtractor.extract(Branch.class, Op.class, name)
                .input("in", type);
        for (Switch s : Switch.values()) {
            builder.output(PropertyName.of(s.name()).toMemberName(), type);
        }
        return builder;
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Branch
        public Switch simple(MockValueModel m) {
            return Switch.valueOf(m.getValue());
        }
    }

    @SuppressWarnings("javadoc")
    public enum Switch {
        A, B, C,
    }
}
