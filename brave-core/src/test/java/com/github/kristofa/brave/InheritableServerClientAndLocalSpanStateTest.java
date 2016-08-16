package com.github.kristofa.brave;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.zipkin.gen.Span;

public class InheritableServerClientAndLocalSpanStateTest {

    private static final short PORT = 80;
    private static final String SERVICE_NAME = InheritableServerClientAndLocalSpanStateTest.class.getSimpleName();
    private static final int MAX_EXPECTED_DEPTH = 1;

    private InheritableServerClientAndLocalSpanState state;
    private ServerSpan mockServerSpan;
    private Span mockSpan;

    @Before
    public void setup() throws UnknownHostException {
        final int ip = InetAddressUtilities.toInt(InetAddress.getByName("192.168.0.1"));
        state = new InheritableServerClientAndLocalSpanState(ip, PORT, SERVICE_NAME);
        mockServerSpan = mock(ServerSpan.class);
        mockSpan = mock(Span.class);
    }

    @After
    public void tearDown() {
        state.setCurrentClientSpan(null);
        state.setCurrentServerSpan(null);
        Span localSpan;
        int depth = 0;
        while ((localSpan = state.getCurrentLocalSpan()) != null) {
            depth++;
            state.setCurrentLocalSpan(null);
            assertThat(state.getCurrentLocalSpan()).isNotEqualTo(localSpan);
            assertThat(depth).isLessThanOrEqualTo(MAX_EXPECTED_DEPTH).as("Depth should not exceed %d, was %d", depth);
        }
        assertThat(state.getCurrentLocalSpan()).isNull();
    }

    @Test
    public void testGetAndSetCurrentServerSpan() {
        assertEquals(ServerSpan.create(null), state.getCurrentServerSpan());
        state.setCurrentServerSpan(mockServerSpan);
        assertSame(mockServerSpan, state.getCurrentServerSpan());
        assertNull("Should not have been modified.", state.getCurrentClientSpan());
    }

    @Test
    public void testGetAndSetCurrentClientSpan() {
        assertNull(state.getCurrentClientSpan());
        state.setCurrentClientSpan(mockSpan);
        assertSame(mockSpan, state.getCurrentClientSpan());
        assertEquals("Should not have been modified.", ServerSpan.create(null),
                state.getCurrentServerSpan());
    }

    @Test
    public void testGetAndSetCurrentLocalSpan() {
        assertNull(state.getCurrentClientSpan());
        assertNull(state.getCurrentLocalSpan());
        state.setCurrentLocalSpan(mockSpan);
        assertSame(mockSpan, state.getCurrentLocalSpan());
        assertEquals("Should not have been modified.", ServerSpan.create(null),
                state.getCurrentServerSpan());
        assertNull(state.getCurrentClientSpan());
    }

    @Test
    public void testGetAndSetCurrentLocalSpanInheritence() {
        assertNull(state.getCurrentClientSpan());
        assertNull(state.getCurrentLocalSpan());
        state.setCurrentLocalSpan(mockSpan);
        assertSame(mockSpan, state.getCurrentLocalSpan());
        assertEquals("Should not have been modified.", ServerSpan.create(null),
                state.getCurrentServerSpan());
        assertNull(state.getCurrentClientSpan());
    }

    @Test
    public void testToString() throws Exception {
        assertThat(state.toString()).startsWith("InheritableServerClientAndLocalSpanState");
    }
}
