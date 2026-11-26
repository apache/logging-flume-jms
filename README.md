<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Welcome to Apache Flume JMS!

Apache Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of event data. It has a simple
and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery
mechanisms. The system is centrally managed and allows for intelligent dynamic
management. It uses a simple extensible data model that allows for online
analytic application.

Apache Flume JMS provides the JMS Source, which reads messages from a JMS queue or topic.
It is selected with the `jms` source type.

Apache Flume JMS is open-sourced under the Apache Software Foundation License v2.0.

## Receiving `ObjectMessage`s

The body of a JMS `ObjectMessage` is a serialized Java object. Deserializing data from
an untrusted source is dangerous: anyone who can publish to the destination could send
a payload that runs code in the Flume agent while it is being deserialized.

The JMS Source therefore only deserializes the classes that are explicitly allowed.
By default these are:

* the boxed primitives (`Boolean`, `Byte`, `Character`, `Short`, `Integer`, `Long`, `Float` and `Double`),
* `String`, `BigInteger` and `BigDecimal`,
* arrays of primitives or of allowed classes.

A message that contains any other class is rejected: a warning naming the rejected class is
logged and the JMS transaction is rolled back, together with the rest of the batch. The broker
then redelivers the message, so a rejected message blocks the destination until the broker's
redelivery policy gives up on it, typically by moving it to a dead letter queue. Make sure
such a policy is configured before relying on rejection.

### Allowing your own classes

To receive `ObjectMessage`s that carry your own classes, implement
`org.apache.flume.source.jms.JMSObjectMessageClassProvider`:

```java
package com.example.flume;

import com.example.model.Order;
import com.example.model.OrderLine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flume.source.jms.JMSObjectMessageClassProvider;

public final class OrderClassProvider implements JMSObjectMessageClassProvider {

    @Override
    public Collection<Class<?>> getAllowedClasses() {
        // `Order` has a `List<OrderLine>` field, holding an `ArrayList`
        return Arrays.asList(Order.class, OrderLine.class, ArrayList.class);
    }
}
```

Providers are discovered with `java.util.ServiceLoader`. Register yours in a file named
`META-INF/services/org.apache.flume.source.jms.JMSObjectMessageClassProvider`:

```
com.example.flume.OrderClassProvider
```

If your code runs on the module path, declare the provider in your `module-info.java` instead:

```java
module com.example.flume {
    requires org.apache.flume.flume.jms.source;
    provides org.apache.flume.source.jms.JMSObjectMessageClassProvider with com.example.flume.OrderClassProvider;
}
```

Put the JAR with your provider and your classes on the agent's class path, for example in
the agent's `plugins.d` directory, next to the JMS Source. The classes allowed by all the
providers that are found are combined. Providers are only loaded once, when the first
`ObjectMessage` is received, so the agent must be restarted to pick up changes.

### Which classes must be registered

The allow-list is checked for **every class in the deserialized object graph**, not just the
class of the message body:

* **Superclasses** of a registered class are allowed automatically, since they are part of
  its serialized form.
* **Field types** must be registered separately. A field declared as `List` that holds an
  `ArrayList` requires `ArrayList`; a `Map` field holding a `HashMap` requires `HashMap`.
  Fields of the default types (`String`, `Integer`, etc.) and primitive fields need nothing.
* **Arrays** are allowed when their element type is primitive or allowed.

Only register classes that are safe to deserialize: never register classes whose
`readObject`, `readResolve` or similar methods have side effects, and avoid registering
broad, general-purpose types you do not need.

### Custom message converters

The JMS Source converts messages with a `JMSMessageConverter`. The default converter applies
the allow-list automatically. A custom converter must call
`JMSObjectMessageFilter.getObject(message)` instead of `message.getObject()`; calling
`getObject()` directly bypasses the allow-list.

### How it works and its limitations

The JMS client library deserializes the body itself, inside `ObjectMessage.getObject()`, so
Flume never sees the serialized stream. The only portable way to restrict it is the JVM-wide
[deserialization filter factory](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/io/ObjectInputFilter.Config.html#setSerialFilterFactory(java.util.function.BinaryOperator))
introduced in Java 17. `JMSObjectMessageFilter` is that factory: it applies the allow-list
only to the streams created by the thread that is reading an `ObjectMessage`, and only while
it does so. All other deserialization in the agent is left to the filters it would have had
otherwise, and a JVM-wide filter set with `-Djdk.serialFilter` still applies on top of the
allow-list.

This has the following limitations:

* **The filter factory can only be installed once per JVM, and only before any object has been
  deserialized.** The JMS Source installs it when it is configured, early in the agent's startup.
  If another component deserialized something first, or another filter factory is already
  installed, the JMS Source logs a warning and **rejects every `ObjectMessage`** rather than
  reading them unfiltered. In that case, install the factory when the JVM starts, for example in
  `JAVA_OPTS` in `flume-env.sh`:

  ```
  -Djdk.serialFilterFactory=org.apache.flume.source.jms.JMSObjectMessageFilter
  ```

  The JMS Source JAR must then be on the JVM's class path. This option cannot be combined with
  another filter factory.
* **The JMS client must deserialize the body on the thread that calls `getObject()`.** This is the
  usual behavior and is verified with ActiveMQ. A client that deserializes messages eagerly on its
  own threads, before Flume reads them, is not protected by the allow-list. Use the JMS client's
  own protections as well where they exist, such as `trustedPackages` in ActiveMQ or the
  `deserializationAllowList` in ActiveMQ Artemis.
* **The event body is the object serialized again**, so whatever consumes the events must also
  deserialize it. Apply the same care there.

`TextMessage`s and `BytesMessage`s are not affected by any of this. If you control the producers,
prefer them to `ObjectMessage`s.

## Documentation

The Flume 2.x guide and FAQ are available here:

* https://cwiki.apache.org/FLUME
* https://cwiki.apache.org/confluence/display/FLUME/Getting+Started

## Contact us!

* Mailing lists: https://cwiki.apache.org/confluence/display/FLUME/Mailing+Lists
* Slack channel #flume on https://the-asf.slack.com/

Bug and Issue tracker.

* https://github.com/apache/logging-flume-jms/issues

## Compiling Flume JMS

Compiling Flume JMS requires the following tools:

* Java JDK 17
* Apache Maven 3.x

Build it with:

```
./mvnw verify
```
