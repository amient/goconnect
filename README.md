# Go Connect

Is a framework which sits somewhere between Kafka Connect and Apache Beam implemented in Go instead of JVM so its 
a lot more efficient and has a low package and memory footprint - it can run happily even on tiny chips.

- it builds linear pipelines for similar to Kafka Connect so it's goal is data connectivity not general data processing 
- it is more general than Kafka Connect and can build file-for-a-file pipelines
- but it is a bit less general compared to Beam it only builds linear chains of transforms, not graphs  
- like Beam, it has internal concept of parallelism and coders however here everything is statically typed 
- it scales simliarly to Kafka Connect by simply running mulitple instances of the same adapter
- it guarantees at-least-once processing at minimum 
  with a choice of optimistic and pessimistic checkpointing depending whether the source supports some notion of offsets or not
- exactly-once guarantees are optional and designed in general similarly to Beam   
- it has a concept of EventTime built in to the basic concept
- it is a unified data processing framework in terms of batch/stream semantics 
  if the input data is bounded the pipeline will terminate when all input elements are fully processed
  if the input data is unbounded the pipeline will run indefinitely 
- it has a first-class support for Avro with Schema Registry
- pipelines have a distinct declaration and materialization phases  
- in the api prototype everything is statically typed but in the final api reflection will be used

(NOTE: THE PROTOTYPE IN THIS CODEBASE DOESN'T HAVE ALL THE FEATURES LISTED ABOVE BUT THOSE ARE THE AIM AND WILL APPEAR SOON)


There are 2 apis: 

1. /goconnect/pkg/api.go       - this is a static api which is used in all the examples so far

2. /goconnect/pkg/goc/api.go   - this is a reflection-based api which is experimental but is the ultimate goal where 
    the the aim is to be able to inject coders and network stages behind the scenes so it looks something like this:

    A. pipeline is declared statically with only those transforms that are required logically for the job
    
    B. pipeline's distributed/parallel aspect is then analysed and network coders are injected accordingly
    
    C. pipeline type analysis is performed and type coders are injected accordingly
    
    D. final pipeline is materialized which starts all routines that wrap around the stage channels
    
    E. materialized pipeline is drained and if all sources are bounded it will eventually terminate
    

Example Pipeline based on the reflection api:
    
    
Declared pipeline:
    
    []:AMQP-QUEUE:[Bytes]													(1) logical
    	>> [Node]:XML-FILTER:[Node]                                         (2) logical
    		>> [Bytes]:STD-OUT:[]                                           (3) logical
                                                                            
Pipeline with injected network coders                                   
                                                                            
    []:AMQP-QUEUE:[Bytes]                                                   (1) logical
    	>> [Bytes]:NET-SPLIT:[Bytes]                                         -  net-out
    		>> [Node]:XML-FILTER:[Node]                                     (2) logical
    			>> [Bytes]:NET-MERGE-SORT:[Bytes]                            -  net-in
    				>> [Bytes]:STD-OUT:[]                                   (3) logical
                                                                               
Final Pipeline with injected type coders                                   
                                                                               
    []:AMQP-QUEUE:[Bytes]                                                   (1) logical
    	>> [Bytes]:NET-SPLIT:[Bytes]                                         -  net-out
    		>> [Bytes]:XML-DECODER:[Node]                                    -  decode
    			>> [Node]:XML-FILTER:[Node]                                 (2) logical
    				>> [Node]:XML-DECODER:[Bytes]                            -  encode
    					>> [Bytes]:NET-MERGE-SORT:[Bytes]                    -  net-in
    						>> [Bytes]:STD-OUT:[]                           (3) logical
    
    
Say 3 instances of this pipelines are executed then this is what has to happen in terms of network
    
    
(1) AMQP Source - can be executed symetrically without coordination but only one of the instances will be active
 -  Net-Split - stamps the input elements and distributes them among the instances usinng round robin strategy 
(2) XML Filter is a pure map tranform so can also be executed symetrically and run in parallel without coordination
 -  Net-Merge-Sort - receives all the outputs and ensures the ordering per source instance
    - it runs only one node - which one is informed by the logical stage, for STD-OUT sink it wouuld make
      sense to run it on the one that joined the group last, as that's where the user will prefer to see the output
(3) STD-OUT - simply runs everywhere since it is the Net-Merge that controls where the output is

So there are several implications of this:
- the default mode of coordination is no coordination - the stage runs everywhere unless constrained otherwise
- Network coders are informed by their surrounding logical stages
- all records must be stamed to:
    a) preserve absolute ordering if required by a network merge stage
    b) provide a means for optimistic checkpointing 
- Nework In Coders are informed by the succeeding stage but there are many subteties to consider e.g.:
    - JMS SOURCE - runs on all and fan-out to all but has to use pessimistic checkpointing becuase in some implementations it is not possible to ack "up to a point"
    - AMQP QUEUE SOURCE - run on all and fan-out to all - it will have only one active instance by the protocol with optimistic checkpoitning as it support delivery tagging
    - KAFKA SOURCE - run on all with no coordiation - kafka consumer is partitioned
    - LOCAL FILE SOURCE - runs on one specific node and does fan-out to all of the file contents  
    - REMOTE FILE SOURCE - will be divided into 2 sub-stages (either explicitly or implicitly)
        - a) list of URLs runs on any one node and fan-out to all
        - b) file contents is retrieved from - runs on all by default  
    - STD-OUT - runs on one specific node selected by the user    
       
     
     
     
    
    
    

