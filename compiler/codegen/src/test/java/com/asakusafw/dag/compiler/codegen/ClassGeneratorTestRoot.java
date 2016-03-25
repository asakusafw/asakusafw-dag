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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.testing.MockClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.utils.common.Action;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test root for class generators.
 */
public abstract class ClassGeneratorTestRoot {

    private static final String TEMPORARY = "com.asakusafw.generating.Temporary";

    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Classpath.
     */
    @Rule
    public final TemporaryFolder classpath = new TemporaryFolder();

    /**
     * Returns a new class generator context.
     * @return the new context
     */
    public ClassGeneratorContext context() {
        return new MockClassGeneratorContext(
                getClass().getClassLoader(),
                classpath());
    }

    /**
     * Returns the classpath container.
     * @return the classpath container
     */
    public ResourceContainer classpath() {
        return new BasicResourceContainer(classpath.getRoot());
    }

    /**
     * Adds a temporary class into the current classpath.
     * @param callback the callback
     * @return the target class
     */
    public ClassDescription add(Generating callback) {
        return add(new ClassDescription(TEMPORARY + counter.incrementAndGet()), callback);
    }

    /**
     * Adds a class into the current classpath.
     * @param target the target class
     * @param callback the callback
     * @return the target class
     */
    public ClassDescription add(ClassDescription target, Generating callback) {
        String path = target.getInternalName() + ".class";
        File file = new File(classpath.getRoot(), path);
        file.getParentFile().mkdirs();
        if (file.exists()) {
            throw new IllegalStateException(path);
        }
        try (OutputStream output = new FileOutputStream(file)) {
            ClassData data = callback.perform(target);
            data.dump(output);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return target;
    }

    /**
     * Loads a generated class.
     * @param description the target class
     * @param action the action for the created class loader
     */
    public void loading(ClassDescription description, Action<Class<?>, ?> action) {
        loading(cl -> {
            Class<?> aClass = description.resolve(cl);
            action.perform(aClass);
        });
    }

    /**
     * Creates a new class loader for loading previously generated classes.
     * @param action the action for the created class loader
     */
    public void loading(Action<ClassLoader, ?> action) {
        try (URLClassLoader loader = URLClassLoader.newInstance(new URL[] { classpath.getRoot().toURI().toURL() })) {
            action.perform(loader);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Creates an adapter object.
     * @param <T> the adapter type
     * @param adapterClass the adapter class
     * @param context the context object
     * @return the created adapter
     */
    @SuppressWarnings("unchecked")
    public <T> T adapter(Class<?> adapterClass, VertexProcessorContext context) {
        try {
            return (T) adapterClass.getConstructor(VertexProcessorContext.class).newInstance(context);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Generating classes callback.
     */
    @FunctionalInterface
    public interface Generating {

        /**
         * Performs class generation.
         * @param target the target class
         * @return the generated class data
         * @throws IOException if I/O exception was occurred
         */
        ClassData perform(ClassDescription target) throws IOException;
    }
}
