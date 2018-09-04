# Go Connect

Is a framework which sits somewhere between Kafka Connect and Apache Beam implemented in Go instead of JVM so its 
a lot more efficient and has a low package and memory footprint - it can run happily even on tiny chips.

- it builds linear pipelines for similar to Kafka Connect so it's goal data connectivity not general data processing 
- it is more general than Kafka Connect and can build file-for-a-file pipelines
- but it is a bit less general compared to Beam it only builds linear chains of transforms, not graphs  
- like Beam, it has internal concept of distributed collections and coders
- it scales simliarly to Kafka Connect by simply running mulitple instances of the same adapter
- it guarantees at-least-once processing at minimum 
- exactly-once guarantees are optional and designed in general similarly to Beam   
- it has a concept of EventTime built in to the basic concept
- it is a unified data processing framework in terms of batch/stream semantics 
  if the input data is bounded the pipeline will terminate when all input elements are fully processed
  if the input data is unbounded the pipeline will run indefinitely 
- it has a first-class support for Avro with Schema Registry
- pipelines have a distinct declaration and materialization phases  


(NOTE: THE PROTOTYPE IN THIS CODEBASE DOESN'T HAVE ALL THE FEATURES LISTED ABOVE BUT THOSE ARE THE AIM AND WILL APPEAR SOON)
