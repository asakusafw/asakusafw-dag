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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.skeleton.CustomVertexProcessor;
import com.asakusafw.dag.utils.common.Arguments;

/**
 * Processes JDBC operations.
 * @since 0.2.0
 */
public class JdbcOperationProcessor extends CustomVertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(JdbcOperationProcessor.class);

    private final List<Spec> specs = new ArrayList<>();

    /**
     * Binds an operation.
     * @param id the operation ID
     * @param driver the operation driver
     * @return this
     */
    public JdbcOperationProcessor bind(String id, JdbcOperationDriver driver) {
        Arguments.requireNonNull(driver);
        return bind(id, context -> driver);
    }

    /**
     * Binds an operation.
     * @param id the operation ID
     * @param provider the operation driver provider
     * @return this
     */
    public JdbcOperationProcessor bind(
            String id,
            Function<? super JdbcContext, ? extends JdbcOperationDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(provider);
        specs.add(new Spec(id, provider));
        return this;
    }

    @Override
    protected List<? extends CustomTaskInfo> schedule(
            VertexProcessorContext context) throws IOException, InterruptedException {
        StageInfo stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        JdbcEnvironment environment = context.getResource(JdbcEnvironment.class)
                .orElseThrow(IllegalStateException::new);
        JdbcContext jdbc = new JdbcContext.Basic(environment, stage::resolveUserVariables);
        List<CustomTaskInfo> results = new ArrayList<>();
        for (Spec spec : specs) {
            JdbcOperationDriver driver = spec.provider.apply(jdbc);
            results.add(c -> {
                driver.perform();
            });
        }
        return results;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "JdbcOperation({0})", //$NON-NLS-1$
                specs.size());
    }

    private static final class Spec {

        final String id;

        final Function<? super JdbcContext, ? extends JdbcOperationDriver> provider;

        Spec(String id, Function<? super JdbcContext, ? extends JdbcOperationDriver> provider) {
            assert id != null;
            assert provider != null;
            this.id = id;
            this.provider = provider;
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
