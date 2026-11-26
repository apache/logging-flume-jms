/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.jms;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.flume.Context;
import org.apache.flume.event.Event;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Deserializes real ActiveMQ object messages, so that the filter applies to the stream created by the JMS client.
 */
public class TestJMSObjectMessageFilter {

    @BeforeClass
    public static void installFilter() {
        assertTrue(JMSObjectMessageFilter.install());
    }

    @Test
    public void allowsDefaultClasses() throws Exception {
        assertEquals(42L, JMSObjectMessageFilter.getObject(serialize(42L)));
        assertEquals("text", JMSObjectMessageFilter.getObject(serialize("text")));
    }

    @Test
    public void allowsRegisteredClassesAndTheirSuperclasses() throws Exception {
        Payload payload = new Payload(42, new int[] {1, 2});
        Payload actual = (Payload) JMSObjectMessageFilter.getObject(serialize(payload));
        assertEquals(payload.value, actual.value);
        assertEquals(payload.name, actual.name);
        assertArrayEquals(payload.numbers, actual.numbers);
        assertTrue(JMSObjectMessageFilter.getAllowedClasses().contains(AbstractPayload.class));
    }

    @Test
    public void rejectsUnregisteredClasses() throws Exception {
        ObjectMessage message = serialize(new UnregisteredPayload());
        assertThrows(JMSException.class, () -> JMSObjectMessageFilter.getObject(message));
    }

    @Test
    public void rejectsUnregisteredClassesInTheObjectGraph() throws Exception {
        ObjectMessage message = serialize(new Payload(new UnregisteredPayload(), null));
        assertThrows(JMSException.class, () -> JMSObjectMessageFilter.getObject(message));
        ObjectMessage list = serialize(new ArrayList<>(Arrays.asList("a", "b")));
        assertThrows(JMSException.class, () -> JMSObjectMessageFilter.getObject(list));
    }

    @Test
    public void onlyFiltersObjectMessages() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new UnregisteredPayload());
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertTrue(in.readObject() instanceof UnregisteredPayload);
        }
    }

    @Test
    public void converterRejectsUnregisteredClasses() throws Exception {
        JMSMessageConverter converter = new DefaultJMSMessageConverter.Builder().build(new Context());
        List<Event> events = converter.convert(serialize(new Payload(7, null)));
        assertEquals(1, events.size());
        assertTrue(events.get(0).getBody().length > 0);
        ObjectMessage message = serialize(new UnregisteredPayload());
        assertThrows(JMSException.class, () -> converter.convert(message));
    }

    /**
     * Returns a message whose body must be deserialized by the JMS client.
     */
    private static ObjectMessage serialize(Serializable object) throws JMSException {
        ActiveMQObjectMessage source = new ActiveMQObjectMessage();
        source.setObject(object);
        source.storeContent();
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setContent(source.getContent());
        return message;
    }

    abstract static class AbstractPayload implements Serializable {
        private static final long serialVersionUID = 1L;

        String name = "payload";
    }

    static final class Payload extends AbstractPayload {
        private static final long serialVersionUID = 1L;

        final Object value;
        final int[] numbers;

        Payload(Object value, int[] numbers) {
            this.value = value;
            this.numbers = numbers;
        }
    }

    static final class UnregisteredPayload implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Registered in {@code META-INF/services}.
     */
    public static final class PayloadClassProvider implements JMSObjectMessageClassProvider {

        @Override
        public Collection<Class<?>> getAllowedClasses() {
            return Collections.singletonList(Payload.class);
        }
    }
}
