package goc


/**
	Checkpoint is a map of int identifiers and values. The identifiers are specific to each transform, some
	may have only one identifier, e.g. AMQP Source, others may have multiple, e.g. Kafka Source

 */

type Checkpoint map[int]interface{}

