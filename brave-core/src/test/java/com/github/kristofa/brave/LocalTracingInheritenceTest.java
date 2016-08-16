package com.github.kristofa.brave;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalTracingInheritenceTest {

    private SpanCollector spanCollector;
    private Sampler sampler;
    private Brave brave;
    private ServerClientAndLocalSpanState state;
    private ThreadFactory threadFactory;

    @Before
    public void setup() throws UnknownHostException {
        final String loggerName = LocalTracingInheritenceTest.class.getName();
        spanCollector = new LoggingSpanCollector(loggerName);
        Logger.getLogger(loggerName).setLevel(Level.ALL);
        sampler = Sampler.ALWAYS_SAMPLE;

        final int ip = InetAddressUtilities.toInt(InetAddressUtilities.getLocalHostLANAddress());
        final String serviceName = LocalTracingInheritenceTest.class.getSimpleName();
        state = new InheritableServerClientAndLocalSpanState(ip, 0, serviceName);
        brave = new Brave.Builder(state)
                .spanCollector(spanCollector)
                .traceSampler(sampler)
                .build();

        threadFactory = new ThreadFactoryBuilder().setNameFormat("brave-%d").build();

        checkState();
    }

    @After
    public void tearDown() throws Exception {
        checkState();
    }

    private void checkState() {
        LocalTracer localTracer = brave.localTracer();
        ServerClientAndLocalSpanState state = localTracer.spanAndEndpoint().state();
        assertThat(state.getCurrentServerSpan()).isSameAs(ServerSpan.EMPTY);
        assertThat(state.getCurrentClientSpan()).isNull();
        assertThat(state.getCurrentLocalSpan()).isNull();
        assertThat(localTracer.spanAndEndpoint().span()).isNull();
    }

    @Test
    public void testGetClientTracer() {
        final ClientTracer clientTracer = brave.clientTracer();
        assertNotNull(clientTracer);
        assertTrue("We expect instance of ClientTracer", clientTracer instanceof ClientTracer);
        assertSame("ClientTracer should be configured with the spancollector we submitted.", spanCollector,
                clientTracer.spanCollector());
        assertSame("ClientTracer should be configured with the traceSampler we submitted.",
                sampler, clientTracer.traceSampler());

        final ClientTracer secondClientTracer = brave.clientTracer();
        assertSame("It is important that each client tracer we get shares same state.",
                clientTracer.spanAndEndpoint().state(), secondClientTracer.spanAndEndpoint().state());
    }

    @Test
    public void testGetServerTracer() {
        final ServerTracer serverTracer = brave.serverTracer();
        assertNotNull(serverTracer);
        assertSame(spanCollector, serverTracer.spanCollector());
        assertSame("ServerTracer should be configured with the traceSampler we submitted.",
                sampler, serverTracer.traceSampler());

        final ServerTracer secondServerTracer = brave.serverTracer();
        assertSame("It is important that each client tracer we get shares same state.",
                serverTracer.spanAndEndpoint().state(), secondServerTracer.spanAndEndpoint().state());
    }

    @Test
    public void testGetLocalTracer() {
        final LocalTracer localTracer = brave.localTracer();
        assertNotNull(localTracer);
        assertSame(spanCollector, localTracer.spanCollector());
        assertSame("LocalTracer should be configured with the traceSampler we submitted.",
                sampler, localTracer.traceSampler());

        final LocalTracer secondLocalTracer = brave.localTracer();
        assertSame("It is important that each local tracer we get shares same state.",
                localTracer.spanAndEndpoint().state(), secondLocalTracer.spanAndEndpoint().state());
    }

    @Test
    public void testStateBetweenServerAndClient() {
        final ClientTracer clientTracer = brave.clientTracer();
        final ServerTracer serverTracer = brave.serverTracer();
        final LocalTracer localTracer = brave.localTracer();

        assertSame("Client and server tracers should share same state.", clientTracer.spanAndEndpoint().state(),
                serverTracer.spanAndEndpoint().state());

        assertSame("Client and local tracers should share same state.", clientTracer.spanAndEndpoint().state(),
                localTracer.spanAndEndpoint().state());

        assertSame("Server and local tracers should share same state.", serverTracer.spanAndEndpoint().state(),
                localTracer.spanAndEndpoint().state());
    }

    @Test
    public void testNestedLocalTraces() throws Exception {
        LocalTracer localTracer = brave.localTracer();

        SpanId span1 = localTracer.startNewSpan("comp1", "op1");
        try {
            SpanId span2 = localTracer.startNewSpan("comp2", "op2");
            try {
                SpanId span3 = localTracer.startNewSpan("comp3", "op3");
                try {
                    SpanId span4 = localTracer.startNewSpan("comp4", "op4");
                    try {
                        assertThat(state.getCurrentLocalSpan().getId()).isEqualTo(span4.spanId);
                    } finally {

                        localTracer.finishSpan();
                    }

                    assertThat(state.getCurrentLocalSpan().getId()).isEqualTo(span3.spanId);
                } finally {
                    localTracer.finishSpan();
                }

                assertThat(state.getCurrentLocalSpan().getId()).isEqualTo(span2.spanId);
            } finally {
                localTracer.finishSpan();
            }

            assertThat(state.getCurrentLocalSpan().getId()).isEqualTo(span1.spanId);
        } finally {
            localTracer.finishSpan();
        }

        assertThat(state.getCurrentLocalSpan()).isNull();

        localTracer.finishSpan(); // unmatched finish should no-op
    }

    @Test
    public void testManyNestedLocalTraces() throws Exception {
        brave = new Brave.Builder(state)
                .spanCollector(spanCollector)
                .traceSampler(Sampler.ALWAYS_SAMPLE)
                .build();

        LocalTracer localTracer = brave.localTracer();

        assertThat(state.getCurrentLocalSpan()).isNull();

        final int limit = 100;
        for (int i = 0; i < 50; i++) {
            for (int index = 1; index < limit; index *= 10) {
                runLocalSpan(index, limit);
            }
        }

        assertThat(state.getCurrentLocalSpan()).isNull();

        localTracer.finishSpan(); // unmatched finish should no-op
    }

    private void runLocalSpan(final int iteration, final int limit) {
        LocalTracer localTracer = brave.localTracer();
        SpanId spanId = localTracer.startNewSpan("comp" + iteration, "op" + iteration);
        try {
            if (iteration < limit) {
                runLocalSpan(iteration + 1, limit);
            }
            assertThat(state.getCurrentLocalSpan().getId()).isEqualTo(spanId.spanId);
        } finally {
            localTracer.finishSpan();
        }
    }

    @Test
    public void testNestedThreads() throws Exception {
        LocalTracer localTracer = brave.localTracer();

        for (int i = 0; i < 4; i++) {
            int threadId = 0;
            SpanId span0 = localTracer.startNewSpan("thread-" + threadId, "run");
            assertThat(span0).isNotNull();
            assertThat(span0.root()).isTrue();
            assertThat(span0.spanId).isEqualTo(span0.traceId);
            assertThat(span0.spanId).isEqualTo(span0.parentId);
            assertThat(span0.nullableParentId()).isNull();

            try {
                runThreads(16, 4);
            } finally {
                localTracer.finishSpan();
            }

            localTracer.finishSpan(); // unmatched finish should no-op
        }
    }

    private void runThreads(int breadth, int depth) throws InterruptedException {
        List<Thread> threads = new ArrayList<Thread>(Math.abs(breadth * depth));
        for (int i = 1; i < breadth; i++) {
            for (int j = 0; j < depth; j++) {
                threads.add(threadFactory.newThread(createRunnable(i, j)));
            }
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private Runnable createRunnable(final int breadth, final int depth) {
        final SpanId baseSpan = brave.localTracer().startNewSpan("thread-" + breadth, "create-" + breadth + ":" + depth);
        assertThat(baseSpan).isNotNull();
        assertThat(baseSpan.nullableParentId()).isNotNull();
        assertThat(baseSpan.root()).isFalse();
        assertThat(baseSpan.spanId).isNotEqualTo(baseSpan.traceId);
        try {
            return new Runnable() {
                @Override
                public void run() {
                    String originalThreadName = Thread.currentThread().getName();
                    Thread.currentThread().setName(originalThreadName + "]"
                            + "[create-" + breadth + ":" + depth + "]");
                    LocalTracer localTracer = brave.localTracer();
                    SpanId runnableSpan = localTracer.startNewSpan("thread-" + breadth + ":" + depth,
                            "run-" + breadth + ":" + depth);
                    assertThat(runnableSpan).isNotNull();
                    assertThat(runnableSpan.nullableParentId()).isNotNull();
                    assertThat(runnableSpan.root()).isFalse();
                    assertThat(runnableSpan.spanId).isNotEqualTo(runnableSpan.traceId);

                    try {
                        runThreads(2, depth - 1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        localTracer.finishSpan();
                        Thread.currentThread().setName(originalThreadName);
                    }
                }
            };
        } finally {
            brave.localTracer().finishSpan();
        }
    }
}
