# Demo Event Hub Application

Demo application that uses Azure Event Hubs client library.

## Run application(s)

1. Set environment variables listed in [Environment.java](./src/main/java/org/example/Environment.java).
   * `EVENTHUB_CONNECTION_STRING`
   * `EVENTHUB_NAME`
   * `CONSUMER_GROUP`
     * Defaults to "$Default".
   * `STORAGE_CONTAINER_NAME`
   * `STORAGE_CONNECTION_STRING`
   * `FOREVER`
      * Anything if you want the sender to run forever.
1. Run any of the main programs.

## Logging

### Configuring Log4J 2

1. Add the following dependencies in your pom.xml.
    ```xml
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-api</artifactId>
        <version>2.14.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-core</artifactId>
        <version>2.14.1</version>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-slf4j-impl</artifactId>
        <version>2.14.1</version>
    </dependency>
    <dependency>
        <groupId>org.codehaus.groovy</groupId>
        <artifactId>groovy-jsr223</artifactId>
        <version>3.0.9</version>
        <scope>runtime</scope>
    </dependency>
    ```
1. Add [log4j2.xml](./src/main/resources/log4j2.xml) to your `src/main/resources`.
1. Add `AZURE_LOG_LEVEL=1` to enable logging for the Azure SDK.

### Configuring logback

1. Add the following dependencies in your pom.xml.
    ```xml
    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-classic</artifactId>
        <version>1.2.6</version>
    </dependency>
    <dependency>
        <groupId>org.codehaus.janino</groupId>
        <artifactId>janino</artifactId>
        <version>3.1.6</version>
    </dependency>
    ```
1. Add [logback.xml](./src/main/resources/logback.xml) to your `src/main/resources`.
1. Add `AZURE_LOG_LEVEL=1` to enable logging for the Azure SDK.