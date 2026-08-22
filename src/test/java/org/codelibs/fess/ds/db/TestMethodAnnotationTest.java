/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.db;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards against JUnit 4 style test methods that JUnit 5 silently ignores.
 *
 * <p>
 * This project runs on utflute, whose {@code PlainTestCase} is Jupiter based:
 * lifecycle hooks are declared with {@code @BeforeEach} / {@code @AfterEach},
 * and there is no JUnit 3 style {@code junit.framework.TestCase} in the
 * hierarchy. A method named {@code test_xxx} is therefore only executed when it
 * carries {@code @Test}. Without it the method compiles, is never run, and the
 * build still reports success - the whole suite can rot to "Tests run: 0"
 * without a single red build.
 * </p>
 */
public class TestMethodAnnotationTest {

    @Test
    public void test_everyTestMethodIsAnnotated() {
        final List<String> missing = new ArrayList<>();
        for (final Class<?> clazz : findTestClasses()) {
            for (final Method method : clazz.getDeclaredMethods()) {
                if (isForgottenTestMethod(method)) {
                    missing.add(clazz.getName() + "#" + method.getName() + "()");
                }
            }
        }
        Assertions.assertTrue(missing.isEmpty(), () -> "The following test methods are missing @Test and would never run: " + missing);
    }

    /**
     * Also fails if class scanning itself breaks, otherwise the guard above
     * would pass vacuously by finding nothing to check.
     */
    @Test
    public void test_scannerFindsTheKnownTestClasses() {
        final List<Class<?>> classes = findTestClasses();
        Assertions.assertTrue(classes.contains(DatabaseDataStoreTest.class),
                () -> "Scanner did not find DatabaseDataStoreTest. Found: " + classes);
    }

    private boolean isForgottenTestMethod(final Method method) {
        return method.getName().startsWith("test_") //
                && Modifier.isPublic(method.getModifiers()) //
                && !Modifier.isStatic(method.getModifiers()) //
                && method.getParameterCount() == 0 //
                && method.getReturnType() == void.class //
                && !method.isAnnotationPresent(Test.class);
    }

    private List<Class<?>> findTestClasses() {
        final Path root;
        try {
            root = Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to locate the test class directory.", e);
        }
        final List<Class<?>> classes = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(path -> path.getFileName().toString().endsWith("Test.class")).forEach(path -> {
                final String name = root.relativize(path).toString().replace(java.io.File.separatorChar, '.');
                try {
                    classes.add(Class.forName(name.substring(0, name.length() - ".class".length())));
                } catch (final ClassNotFoundException | NoClassDefFoundError e) {
                    throw new IllegalStateException("Failed to load a test class: " + name, e);
                }
            });
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to scan " + root, e);
        }
        return classes;
    }
}
