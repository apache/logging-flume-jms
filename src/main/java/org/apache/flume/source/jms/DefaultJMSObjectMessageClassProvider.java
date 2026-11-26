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

import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceProvider;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

/**
 * Allows the JDK value types that are safe to deserialize: boxed primitives, {@link String},
 * {@link BigInteger} and {@link BigDecimal}.
 */
@ServiceProvider(value = JMSObjectMessageClassProvider.class, resolution = Resolution.OPTIONAL)
public final class DefaultJMSObjectMessageClassProvider implements JMSObjectMessageClassProvider {

    @Override
    public Collection<Class<?>> getAllowedClasses() {
        return Arrays.asList(
                Boolean.class,
                Byte.class,
                Character.class,
                Short.class,
                Integer.class,
                Long.class,
                Float.class,
                Double.class,
                String.class,
                BigInteger.class,
                BigDecimal.class);
    }
}
