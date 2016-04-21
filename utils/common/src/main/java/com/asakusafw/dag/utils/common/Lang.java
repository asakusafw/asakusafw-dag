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
package com.asakusafw.dag.utils.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Core language extensions.
 */
public final class Lang {

    private Lang() {
        return;
    }

    /**
     * Does nothing.
     * @param <T> the return type
     * @return {@code null}
     */
    public static <T> T pass() {
        return pass(null);
    }

    /**
     * Does nothing.
     * @param <T> the return type
     * @param value the value
     * @return the value
     */
    public static <T> T pass(T value) {
        return value;
    }

    /**
     * Discards passed values.
     * @param <T> the value type
     * @return the consumer
     */
    public static <T> Consumer<T> discard() {
        return t -> {
            return;
        };
    }

    /**
     * Assumes that the action does not throw any exceptions.
     * @param <T> the return type
     * @param action the target action
     * @return the result of {@link Callable#call() callable.call()}
     * @throws AssertionError if the action throws an exception
     */
    public static <T> T safe(Callable<T> action) {
        Objects.requireNonNull(action);
        try {
            return action.call();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Assumes that the action does not throw any exceptions.
     * @param action the target action
     * @throws AssertionError if the action throws an exception
     */
    public static void safe(RunnableWithException<?> action) {
        Objects.requireNonNull(action);
        try {
            action.run();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Applies the {@code value} to action, and returns the original {@code value}.
     * @param <T> the value type
     * @param <E> the throwable exception type
     * @param value the value
     * @param action the action for the value
     * @return the original value
     * @throws E if an exception was occurred while performing the action
     */
    public static <T, E extends Exception> T let(T value, Action<? super T, E> action) throws E {
        Objects.requireNonNull(action);
        action.perform(value);
        return value;
    }

    /**
     * Executes a {@link CallableWithException}.
     * @param <T> the result type
     * @param <E> the throwable exception type
     * @param action the action
     * @return the action result
     * @throws E if an exception was occurred while performing the action
     */
    public static <T, E extends Exception> T compute(CallableWithException<T, E> action) throws E {
        Objects.requireNonNull(action);
        return action.call();
    }

    /**
     * Performs the action with the specified times.
     * @param <E> the throwable exception type
     * @param count the number of that the action should be performed
     * @param action the target action
     * @throws E if an exception was occurred
     */
    public static <E extends Exception> void repeat(int count, RunnableWithException<? extends E> action) throws E {
        Objects.requireNonNull(action);
        for (int i = 0; i < count; i++) {
            action.run();
        }
    }

    /**
     * Performs the action with the specified times.
     * @param count the number of that the action should be performed
     * @param action the target action
     */
    public static void repeat(int count, IntConsumer action) {
        Objects.requireNonNull(action);
        for (int i = 0; i < count; i++) {
            action.accept(i);
        }
    }

    /**
     * Iterates elements over the {@code array}.
     * @param <T> the element type
     * @param <E> the throwable exception type
     * @param array the target array
     * @param action the action for each element
     * @throws E if an exception was occurred
     */
    public static <T, E extends Exception> void forEach(
            T[] array,
            Action<? super T, ? extends E> action) throws E {
        Objects.requireNonNull(array);
        Objects.requireNonNull(action);
        for (int i = 0; i < array.length; i++) {
            action.perform(array[i]);
        }
    }

    /**
     * Iterates elements over the {@code iterable}.
     * @param <T> the element type
     * @param <E> the throwable exception type
     * @param iterable the target iterable
     * @param action the action for each element
     * @throws E if an exception was occurred
     */
    public static <T, E extends Exception> void forEach(
            Iterable<T> iterable,
            Action<? super T, ? extends E> action) throws E {
        Objects.requireNonNull(iterable);
        Objects.requireNonNull(action);
        for (T value : iterable) {
            action.perform(value);
        }
    }

    /**
     * Returns a projection of the collection.
     * @param <TInput> the input element type
     * @param <TOutput> the output element type
     * @param input the input collection
     * @param mapping mapping function
     * @return the mapped list
     */
    public static <TInput, TOutput> List<TOutput> project(
            Collection<? extends TInput> input,
            Function<? super TInput, ? extends TOutput> mapping) {
        Objects.requireNonNull(input);
        Objects.requireNonNull(mapping);
        return input.stream().map(mapping).collect(Collectors.toList());
    }

    /**
     * Returns a projection of the array.
     * @param <TInput> the input element type
     * @param <TOutput> the output element type
     * @param input the input array
     * @param mapping mapping function
     * @return the mapped list
     */
    public static <TInput, TOutput> List<TOutput> project(
            TInput[] input,
            Function<? super TInput, TOutput> mapping) {
        Objects.requireNonNull(input);
        Objects.requireNonNull(mapping);
        return Stream.of(input).map(mapping).collect(Collectors.toList());
    }

    /**
     * Concatenates two collections.
     * @param <T> the element type
     * @param left the left collection
     * @param right the right collection
     * @return the concatenated list
     */
    public static <T> List<T> concat(Collection<? extends T> left, Collection<? extends T> right) {
        List<T> results = new ArrayList<>();
        results.addAll(left);
        results.addAll(right);
        return results;
    }

    /**
     * Re-throws the throwable only if the throwable object is instance of the specified type.
     * @param <T> the re-throw type
     * @param throwable the target throwable object
     * @param type the re-throw type
     * @throws T if the throwable object is instance of the specified type
     */
    public static <T extends Throwable> void rethrow(Throwable throwable, Class<T> type) throws T {
        if (type.isInstance(throwable)) {
            throw type.cast(throwable);
        }
    }
}
