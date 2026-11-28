package com.taskprocessor.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CorrelationIdFilterTest {
    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    private String run(String header, MockHttpServletResponse response) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (header != null) {
            request.addHeader(CorrelationIdFilter.HEADER, header);
        }
        AtomicReference<String> seen = new AtomicReference<>();
        FilterChain chain = (req, res) -> seen.set(MDC.get(CorrelationIdFilter.MDC_KEY));
        filter.doFilter(request, response, chain);
        return seen.get();
    }

    @Test void reusesSuppliedIdEchoesItAndClearsContext() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertEquals("order-123", run("order-123", response));
        assertEquals("order-123", response.getHeader(CorrelationIdFilter.HEADER));
        assertNull(MDC.get(CorrelationIdFilter.MDC_KEY));
    }

    @Test void generatesAnIdWhenMissing() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        String id = run(null, response);
        assertNotNull(id);
        assertEquals(id, response.getHeader(CorrelationIdFilter.HEADER));
    }

    @Test void replacesMalformedIdsInsteadOfTrustingThem() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        String id = run("bad id\nwith injection", response);
        assertNotEquals("bad id\nwith injection", id);
        assertTrue(id.matches("[0-9a-f-]{36}"));
    }
}
