### AMQP Protocol Feature Matrix

| feature          | single-bundle | multi-bundles | description                          |
|------------------|---------------|---------------|--------------------------------------|
| exchange declare | supported     | supported     |                                      |
| exchange delete  | supported     | not supported |                                      |
| exchange bound   | supported     | not supported | judge whether the exchange has bound |
| queue declare    | supported     | supported     |                                      |
| queue bind       | supported     | supported     |                                      |
| queue purge      | not supported | not supported |                                      |
| queue delete     | supported     | not supported |                                      |
| queue unbind     | supported     | supported     |                                      |
| basic recover    | supported     | not supported |                                      |
| basic qos        | supported     | not supported |                                      |
| basic consume    | supported     | supported     |                                      |
| basic cancel     | supported     | not supported |                                      |
| basic publish    | supported     | supported     |                                      |
| basic get        | supported     | not supported |                                      |
| basic ack        | supported     | not supported |                                      |
| basic reject     | supported     | not supported |                                      |
| basic nack       | supported     | not supported |                                      |
| txn select       | not supported | not supported |                                      |
| txn commit       | not supported | not supported |                                      |
| txn rollback     | not supported | not supported |                                      |
| confirm select   | not supported | not supported |                                      |

### RabbitMQ Plugin

| plugin          | single-bundle | multi-bundles | description |
|-----------------|---------------|-------not supported--------|-------------|
| delayed message | supported     |  |             |

### Authentication

| mechanism | single-bundle | multi-bundles | description |
|-----------|---------------|---------------|-------------|
| PLAIN     | supported     | supported     |             |
| token     | supported     | supported     |             |

### Authorization

Not supported
