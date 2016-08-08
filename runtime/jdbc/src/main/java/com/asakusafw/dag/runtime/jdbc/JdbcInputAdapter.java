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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver.Partition;
import com.asakusafw.dag.utils.common.Arguments;

/**
 * {@link InputAdapter} for JDBC inputs.
 * @since 0.2.0
 */
public class JdbcInputAdapter implements InputAdapter<ExtractOperation.Input> {

    private final JdbcContext jdbc;

    private final List<Spec> specs = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public JdbcInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        StageInfo stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        JdbcEnvironment environment = context.getResource(JdbcEnvironment.class)
                .orElseThrow(IllegalStateException::new);
        this.jdbc = new JdbcContext.Basic(environment, stage::resolveUserVariables);
    }

    /**
     * Adds an input.
     * @param id the input ID
     * @param driver the input driver
     * @return this
     */
    public final JdbcInputAdapter bind(String id, JdbcInputDriver driver) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(driver);
        return bind(id, context -> driver);
    }

    /**
     * Adds an input.
     * @param id the input ID
     * @param provider the input driver provider
     * @return this
     */
    public final JdbcInputAdapter bind(String id, Function<? super JdbcContext, ? extends JdbcInputDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(provider);
        specs.add(new Spec(id, provider));
        return this;
    }

    @Override
    public TaskSchedule getSchedule() throws IOException, InterruptedException {
        List<Task> tasks = new ArrayList<>();
        for (Spec spec : specs) {
            JdbcInputDriver driver = spec.provider.apply(jdbc);
            List<? extends Partition> partitions = driver.getPartitions();
            for (JdbcInputDriver.Partition partition : partitions) {
                tasks.add(new Task(partition));
            }
        }
        return new BasicTaskSchedule(tasks);
    }

    @Override
    public InputHandler<Input, TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        return context -> context.getTaskInfo()
                .map(Task.class::cast)
                .orElseThrow(IllegalStateException::new)
                .newDriver();
    }

    @Override
    public String toString() {
        return String.format("JdbcInput(%s)", specs); //$NON-NLS-1$
    }

    private static class Spec {

        final String id;

        final Function<? super JdbcContext, ? extends JdbcInputDriver> provider;

        Spec(String id, Function<? super JdbcContext, ? extends JdbcInputDriver> provider) {
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

    private static class Task implements TaskInfo {

        private final JdbcInputDriver.Partition partition;

        Task(Partition partition) {
            Arguments.requireNonNull(partition);
            this.partition = partition;
        }

        Driver newDriver() throws IOException, InterruptedException {
            return new Driver(partition.open());
        }
    }

    private static final class Driver
            implements InputSession<ExtractOperation.Input>, ExtractOperation.Input {

        private final ObjectReader reader;

        Driver(ObjectReader reader) {
            assert reader != null;
            this.reader = reader;
        }

        @Override
        public ExtractOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            return reader.nextObject();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <S> S getObject() throws IOException, InterruptedException {
            return (S) reader.getObject();
        }

        @Override
        public void close() throws IOException, InterruptedException {
            reader.close();
        }
    }
}
