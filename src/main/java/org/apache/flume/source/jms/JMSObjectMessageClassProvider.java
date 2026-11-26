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

import java.util.Collection;

/**
 * Registers the classes that may be deserialized from the body of a {@link javax.jms.ObjectMessage}.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader}: list them in
 * {@code META-INF/services/org.apache.flume.source.jms.JMSObjectMessageClassProvider}.
 * {@link JMSObjectMessageFilter} rejects every class of the deserialized object graph that no provider
 * registers, so a message can only carry instances of the registered classes.</p>
 *
 * <p>Registering a class also allows its serializable superclasses, which are part of its serialized form.
 * The types of its serialized fields, such as collections, must be registered separately, unless they are
 * one of the types allowed by default (boxed primitives, {@link String}, {@link java.math.BigInteger} and
 * {@link java.math.BigDecimal}). Arrays are allowed if their element type is primitive or allowed.</p>
 *
 * <p>Only register classes whose deserialization cannot be abused: in particular, never register classes
 * whose {@code readObject}, {@code readResolve} or similar methods have side effects.</p>
 */
public interface JMSObjectMessageClassProvider {

    /**
     * Returns the classes whose instances may be deserialized from an {@link javax.jms.ObjectMessage}.
     */
    Collection<Class<?>> getAllowedClasses();
}
