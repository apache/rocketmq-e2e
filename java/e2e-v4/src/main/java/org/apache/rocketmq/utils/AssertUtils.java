/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.utils;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AssertUtils {
    private static final Logger log = LoggerFactory.getLogger(AssertUtils.class);
//    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

    public static void assertConcurrent(final String message, final List<? extends Runnable> runnables,
        final int maxTimeoutSeconds) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(new Runnable() {
                    public void run() {
                        allExecutorThreadsReady.countDown();
                        try {
                            afterInitBlocker.await();
                            submittedTestRunnable.run();
                        } catch (final Throwable e) {
                            exceptions.add(e);
                        } finally {
                            allDone.countDown();
                        }
                    }
                });
            }
            // wait until all threads are ready
            Assertions.assertTrue(allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS), "Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent");
            // start all test runners
            afterInitBlocker.countDown();
            Assertions.assertTrue(allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS), message + " timeout! More than " + maxTimeoutSeconds + " seconds");
        } finally {
            log.info("exit sub thread");
            threadPool.shutdown();
        }
        Assertions.assertTrue(exceptions.isEmpty(), message + " failed with exception(s)" + exceptions);
    }

    public static void assertConcurrentAtMost(final String message, final List<? extends Runnable> runnables,
        final int maxTimeoutSeconds) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(new Runnable() {
                    public void run() {
                        allExecutorThreadsReady.countDown();
                        try {
                            afterInitBlocker.await();
                            submittedTestRunnable.run();
                        } catch (final Throwable e) {
                            exceptions.add(e);
                        } finally {
                            allDone.countDown();
                        }
                    }
                });
            }
            // wait until all threads are ready
            Assertions.assertTrue(allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS), "Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent");
            // start all test runners
            afterInitBlocker.countDown();
            allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS);
//            Assertions.assertTrue(allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS), message + " timeout! More than " + maxTimeoutSeconds + " seconds");
        } finally {
            log.info("exit sub thread");
            threadPool.shutdown();
        }
        Assertions.assertTrue(exceptions.isEmpty(), message + " failed with exception(s)" + exceptions);
    }
}
