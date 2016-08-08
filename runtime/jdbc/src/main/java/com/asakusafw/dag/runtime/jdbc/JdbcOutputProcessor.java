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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.utils.common.Arguments;
import com.asakusafw.dag.utils.common.Invariants;

/**
 * Processes JDBC output.
 * @since 0.2.0
 */
public class JdbcOutputProcessor implements VertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(JdbcOutputProcessor.class);

    /**
     * The input edge name.
     */
    public static final String INPUT_NAME = "input";

    private IoCallable<TaskProcessor> lazy;

    private Spec spec;

    /**
     * Binds an output.
     * @param id the output ID
     * @param driver the output driver
     * @return this
     */
    public JdbcOutputProcessor bind(String id, JdbcOutputDriver driver) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(driver);
        return bind(id, context -> driver);
    }

    /**
     * Binds an output.
     * @param id the output ID
     * @param provider the output driver provider
     * @return this
     */
    public JdbcOutputProcessor bind(String id, Function<? super JdbcContext, ? extends JdbcOutputDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(provider);
        Invariants.require(spec == null);
        spec = new Spec(id, provider);
        return this;
    }

    @Override
    public Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        Invariants.require(spec != null);
        Invariants.require(lazy == null);

        StageInfo stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        JdbcEnvironment environment = context.getResource(JdbcEnvironment.class)
                .orElseThrow(IllegalStateException::new);

        JdbcOutputDriver driver = spec.provider.apply(new JdbcContext.Basic(environment, stage::resolveUserVariables));
        driver.initialize();

        this.lazy = () -> new Task(spec.id, driver);

        return Optional.empty();
    }

    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        return lazy.call();
    }

    private static final class Spec {

        final String id;

        final Function<? super JdbcContext, ? extends JdbcOutputDriver> provider;

        Spec(String id, Function<? super JdbcContext, ? extends JdbcOutputDriver> provider) {
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

    private static final class Task implements TaskProcessor {

        private final String id;

        private final JdbcOutputDriver driver;

        private ObjectWriter output;

        Task(String id, JdbcOutputDriver driver) {
            this.id = id;
            this.driver = driver;
        }

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            if (output == null) {
                LOG.debug("starting JDBC output: {} ({})", id, driver);
                output = driver.open();
            }
            try (ObjectReader reader = (ObjectReader) context.getInput(INPUT_NAME)) {
                while (reader.nextObject()) {
                    Object obj = reader.getObject();
                    output.putObject(obj);
                }
            } catch (Throwable t) {
                LOG.error(MessageFormat.format(
                        "error occurred while writing output: {0}",
                        id), t);
                throw t;
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            if (output != null) {
                output.close();
                output = null;
            }
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "JdbcOutput(id={0}, driver={1})", //$NON-NLS-1$
                    id, driver);
        }
    }
}
