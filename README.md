## Pulsar IO :: IoTdb

This is a IoTdb Source implementation that can receive data from IoTdb tsfile, and then publish to Pulsar.

### Installation

1. Start Pulsar standalone

```$xslt
./bin/pulsar standalone -a 127.0.0.1 -nss
```

2. Clone code and build.

```$xslt
git clone https://github.com/tuteng/pulsar-iotdb
cd pulsar-iotdb
mvn clean install
```

3. Start connector source

```$xslt
./bin/pulsar-admin sources localrun -a target/pulsar-iotdb-0.0.1.nar --destination-topic-name iotdb --source-config-file src/main/resources/iotdb-source-config.yaml --name test-iotdb
```

4. Consume from thie pulsar topic

```$xslt
./bin/pulsar-client consume -s "iotdb-name" iotdb -n 0
``` 

### Configuration

<!-- write instruction of how to configure this connector -->

### Examples

<!-- provide an example of how to use this connector -->

### Monitoring

<!-- provide instructions of how to monitoring this connector if there is any -->

### Troubleshooting

<!-- provide instructions of how to troubleshoot the problems occur to this connector -->

### License

<!-- describe the license of this connector -->
