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

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import java.io.ObjectInputFilter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BinaryOperator;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Restricts the deserialization of {@link ObjectMessage} bodies to the classes registered by
 * {@link JMSObjectMessageClassProvider} services.
 *
 * <p>The JMS client deserializes the body of an {@link ObjectMessage} itself, inside
 * {@link ObjectMessage#getObject()}, so the only portable way to restrict it is a
 * {@linkplain ObjectInputFilter.Config#setSerialFilterFactory JVM-wide deserialization filter factory}.
 * This class is that factory: it adds the allow-list to the streams created by the current thread while
 * {@link #getObject(ObjectMessage)} runs, and leaves all other streams to the filters they would have had
 * otherwise. Any JVM-wide filter set with {@code jdk.serialFilter} still applies.</p>
 *
 * <p>The JDK only accepts a filter factory before the first {@link java.io.ObjectInputStream} is created.
 * The JMS source installs it when it is configured; if that is too late, or another factory is installed,
 * start the JVM with {@code -Djdk.serialFilterFactory=org.apache.flume.source.jms.JMSObjectMessageFilter}
 * (this requires the JMS source on the class path). Without the factory, {@link #getObject(ObjectMessage)}
 * refuses to deserialize anything.</p>
 *
 * <p>The filter only protects JMS clients that deserialize the body on the thread that calls
 * {@link ObjectMessage#getObject()}, which is the usual behavior.</p>
 */
@ServiceConsumer(
        value = JMSObjectMessageClassProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public final class JMSObjectMessageFilter implements BinaryOperator<ObjectInputFilter> {

    private static final Logger LOGGER = LogManager.getLogger(JMSObjectMessageFilter.class);

    private static final ThreadLocal<ObjectInputFilter> ACTIVE_FILTER = new ThreadLocal<>();

    private static volatile Boolean installed;

    private final BinaryOperator<ObjectInputFilter> delegate;

    /**
     * Creates the filter factory, when it is set with the {@code jdk.serialFilterFactory} system property.
     */
    public JMSObjectMessageFilter() {
        this(null);
    }

    private JMSObjectMessageFilter(BinaryOperator<ObjectInputFilter> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ObjectInputFilter apply(ObjectInputFilter current, ObjectInputFilter requested) {
        // Without a delegate, behave as the builtin JDK factory
        ObjectInputFilter filter = delegate != null ? delegate.apply(current, requested) : requested;
        ObjectInputFilter active = ACTIVE_FILTER.get();
        if (active == null) {
            return filter;
        }
        // A class is rejected if either filter rejects it
        return filter != null ? ObjectInputFilter.merge(active, filter) : active;
    }

    /**
     * Installs this class as the JVM-wide deserialization filter factory, unless it is already installed.
     *
     * @return {@code true} if the filter factory is installed.
     */
    public static boolean install() {
        Boolean result = installed;
        if (result == null) {
            synchronized (JMSObjectMessageFilter.class) {
                result = installed;
                if (result == null) {
                    result = doInstall();
                    installed = result;
                }
            }
        }
        return result;
    }

    private static boolean doInstall() {
        BinaryOperator<ObjectInputFilter> current = ObjectInputFilter.Config.getSerialFilterFactory();
        if (current instanceof JMSObjectMessageFilter) {
            return true;
        }
        try {
            ObjectInputFilter.Config.setSerialFilterFactory(new JMSObjectMessageFilter(current));
            LOGGER.debug("Installed the JMS ObjectMessage deserialization filter factory");
            return true;
        } catch (IllegalStateException | SecurityException e) {
            LOGGER.warn(
                    "Unable to install the deserialization filter factory: ObjectMessage bodies will be rejected. "
                            + "Start the JVM with -Djdk.serialFilterFactory={} to accept them.",
                    JMSObjectMessageFilter.class.getName(),
                    e);
            return false;
        }
    }

    /**
     * Deserializes the body of an {@link ObjectMessage}, allowing only the classes registered by
     * {@link JMSObjectMessageClassProvider} services.
     *
     * <p>Custom {@link JMSMessageConverter}s should use this method instead of {@link ObjectMessage#getObject()}.</p>
     *
     * @param message an object message.
     * @return the body of the message.
     * @throws JMSException if the body contains a class that is not allowed or cannot be deserialized.
     */
    public static Serializable getObject(ObjectMessage message) throws JMSException {
        if (!install()) {
            throw new JMSException("ObjectMessage deserialization is disabled, since the deserialization filter "
                    + "factory could not be installed. Start the JVM with -Djdk.serialFilterFactory="
                    + JMSObjectMessageFilter.class.getName());
        }
        ObjectInputFilter previous = ACTIVE_FILTER.get();
        ACTIVE_FILTER.set(AllowedClasses.FILTER);
        try {
            return message.getObject();
        } finally {
            if (previous != null) {
                ACTIVE_FILTER.set(previous);
            } else {
                ACTIVE_FILTER.remove();
            }
        }
    }

    /**
     * Returns the classes allowed by the {@link JMSObjectMessageClassProvider} services, including their
     * serializable superclasses.
     */
    static Set<Class<?>> getAllowedClasses() {
        return AllowedClasses.FILTER.allowed;
    }

    private static final class AllowedClasses implements ObjectInputFilter {

        // Lazily loads the providers
        private static final AllowedClasses FILTER = new AllowedClasses(loadAllowedClasses());

        private final Set<Class<?>> allowed;

        private AllowedClasses(Set<Class<?>> allowed) {
            this.allowed = allowed;
        }

        @Override
        public Status checkInput(FilterInfo filterInfo) {
            Class<?> clazz = filterInfo.serialClass();
            if (clazz == null) {
                // Only checks the limits of the graph
                return Status.UNDECIDED;
            }
            while (clazz.isArray()) {
                clazz = clazz.getComponentType();
            }
            if (clazz.isPrimitive() || allowed.contains(clazz)) {
                return Status.ALLOWED;
            }
            LOGGER.warn(
                    "Rejected the deserialization of {}: register it with a {} service",
                    clazz.getName(),
                    JMSObjectMessageClassProvider.class.getName());
            return Status.REJECTED;
        }

        private static Set<Class<?>> loadAllowedClasses() {
            Set<Class<?>> allowed = new HashSet<>();
            Iterator<JMSObjectMessageClassProvider> providers = ServiceLoader.load(
                            JMSObjectMessageClassProvider.class, JMSObjectMessageFilter.class.getClassLoader())
                    .iterator();
            while (true) {
                JMSObjectMessageClassProvider provider;
                try {
                    if (!providers.hasNext()) {
                        break;
                    }
                    provider = providers.next();
                } catch (ServiceConfigurationError e) {
                    LOGGER.warn("Unable to load a {} service", JMSObjectMessageClassProvider.class.getName(), e);
                    continue;
                }
                Collection<Class<?>> classes = provider.getAllowedClasses();
                if (classes != null) {
                    for (Class<?> clazz : classes) {
                        addWithSuperclasses(allowed, clazz);
                    }
                }
                LOGGER.debug(
                        "Loaded the ObjectMessage classes of {}",
                        provider.getClass().getName());
            }
            return Collections.unmodifiableSet(allowed);
        }

        private static void addWithSuperclasses(Set<Class<?>> allowed, Class<?> clazz) {
            for (Class<?> current = clazz;
                    current != null && Serializable.class.isAssignableFrom(current);
                    current = current.getSuperclass()) {
                allowed.add(current);
            }
        }
    }
}
