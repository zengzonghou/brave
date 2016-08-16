package com.github.kristofa.brave;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;

import com.github.kristofa.brave.internal.Util;
import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;

/**
 * {@link ServerClientAndLocalSpanState} implementation that keeps trace state using {@link InheritableThreadLocal}
 * variables and a {@link Deque} of local spans.
 */
public final class InheritableServerClientAndLocalSpanState implements ServerClientAndLocalSpanState {

    private final InheritableThreadLocal<ServerSpan> currentServerSpan =
            new InheritableThreadLocal<ServerSpan>() {
                @Override
                protected ServerSpan initialValue() {
                    return ServerSpan.EMPTY;
                }
            };

    private final InheritableThreadLocal<Span> currentClientSpan = new InheritableThreadLocal<>();

    private final InheritableThreadLocal<Deque<Span>> currentLocalSpan =
            new InheritableThreadLocal<Deque<Span>>() {
                @Override
                protected Deque<Span> initialValue() {
                    return new LinkedBlockingDeque<Span>();
                }
            };

    private final Endpoint endpoint;

    /**
     * Constructor
     *
     * @param ip Int representation of ipv4 address.
     * @param port port on which current process is listening.
     * @param serviceName Name of the local service being traced. Should be lowercase and not <code>null</code> or empty.
     */
    public InheritableServerClientAndLocalSpanState(int ip, int port, String serviceName) {
        Util.checkNotBlank(serviceName, "Service name must be specified.");
        endpoint = Endpoint.create(serviceName, ip, port);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerSpan getCurrentServerSpan() {
        return currentServerSpan.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCurrentServerSpan(final ServerSpan span) {
        if (span == null) {
            currentServerSpan.remove();
        } else {
            currentServerSpan.set(span);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Endpoint endpoint() {
        return endpoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Span getCurrentClientSpan() {
        return currentClientSpan.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCurrentClientSpan(final Span span) {
        currentClientSpan.set(span);
    }

    @Override
    public Boolean sample() {
        return getCurrentServerSpan().getSample();
    }

    @Override
    public Span getCurrentLocalSpan() {
        return currentLocalSpan.get().peekFirst();
    }

    @Override
    public void setCurrentLocalSpan(Span span) {
        Deque<Span> deque = currentLocalSpan.get();
        if (span == null) {
            // pop to remove
            deque.pollFirst();
        } else {
            deque.addFirst(span);
        }
    }

    @Override
    public String toString() {
        return "InheritableServerClientAndLocalSpanState{"
                + "endpoint=" + endpoint + ", "
                + "currentLocalSpan=" + currentLocalSpan + ", "
                + "currentClientSpan=" + currentClientSpan + ", "
                + "currentServerSpan=" + currentServerSpan
                + "}";
    }
}
