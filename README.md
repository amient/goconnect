# Go Connect

Is a framework which sits somewhere between Kafka Connect and Apache Beam implemented in Go instead of JVM so its 
a lot more efficient and has a low package and memory footprint - it can run happily even on tiny chips.

- it builds linear pipelines for similar to Kafka Connect so it's goal is data connectivity not general data processing 
- it is more general than Kafka Connect and can build file-for-a-file pipelines
- but it is a bit less general compared to Beam it only builds linear chains of transforms, not graphs
- it is also stateless at the moment
- like Beam, it has internal concept of parallelism and coders
- additionally it features vertical and horizontal parallelism out-of-the-box
  - (roots cannot have vertical parallelism greater than 1)
- it scales similarly to Kafka Connect by simply running multiple identical instances 
- it guarantees at-least-once processing and is capable of exactly-once 
  with a choice of optimistic and pessimistic checkpointing depending whether the source supports some notion of offsets or not
- it has a concept of EventTime built-in
- it is a unified data processing framework in terms of batch/stream semantics 
  if the input data is bounded the pipeline will terminate when all input elements are fully processed
  if the input data is unbounded the pipeline will run indefinitely 
- it has a first-class support for Avro with Schema Registry
- pipelines have a distinct declaration and materialization phases

(NOTE: THE PROTOTYPE IN THIS CODEBASE DOESN'T HAVE ALL THE FEATURES LISTED ABOVE BUT THOSE ARE THE AIM AND WILL APPEAR SOON)

## Implemented Features

- Generalized at-least-once processing guarantees for all element wise processing, i.e. covers all features below
- Declaration vs Materialization - pipeline is declared as a graph of Defs and then materialized into a runnable graph
- Bounded and Unbounded sources are treated identically both in the API and in the runner context - stream and batch totally unified
- Network Runner: network.Runner(pipeline, peers..) - allows stages to place network constraints by requesting receivers/senders and applying network logic
- Single Runner: Pipeline.Run() - for pipelines whose stages don't have any network constraints and can run in parallel without coordination
- TriggerEach / TriggerEvery for SinkFn - either may be used optionally for performance gain, default trigger is no trigger, i.e. only one at the end if 
- TriggerEach / TriggerEvery for FoldFn - either must be used in order for fold to emit anything
- Configurable stage buffers: .Buffer(numElements int) - used to control pending/ack buffers, not output buffers
- Any stage except Root can have veritical paralleism set by .Par(n) with guaranteed ordering
  - i.e. how many routines run the fn in parallel and push to the output channel 
- Stage output can be limited to n elements .Limit(n) - this makes any pipeline bounded 
- Coders are injected recursively using coder.Registry()
- Coder vertical parallelism can be controlled by Pipeline.CoderPar(n) default setting
- Coders
  - binary > xml > binary
  - gzip > binary > gzip
  - string > binary > string
  - avro generic decoder (binary)
  - avro generic encoder (binary or json)  
  - avro generic projector 
  - avro schema registry decoder 
  - avro schema registry encoder
- Sources (Root Transforms) 
  - List Source / RoundRobin Source
  - Amqp09 Source
  - Kafka Source
  - File Source
- Transforms
    - SinkFn(func)
    - FoldFn(func)
    - FilterFn + UserFilterFn(func)
    - MapFn + UserMapFn(func)
    - FlatMapFn + UserFlatMapFn(func)
    - FoldFn +UserFoldFn(func) + .Count()
    - Text (used to flatMap files into lines)
- Sinks
  - Kafka Sink
  - Amqp09 Sink
  - StdOut Sink
- Network
  - RoundRobin
  - MergeOrdered

## Features In Progress/TODO
- refactor root to materialized 
  - migrate all transforms which use Context.Put/Get to Materialization 
- file source using network split internally to spread the URLs 
- subflows
    - file-for-a-file use case
    - avro applied to kv pairs: goc.KVBinary -> avro.KVGenericDecoder -> avro.KVBinary -> SchemaRegistryKVEncoder(topic) -> goc.KVBinary
- persistent checkpoints
- avro: specific decoder and encoder
- processing epochs: dynamic node join/leave potentially using Raft algo
- coder injection shortest path in case there are mutliple combinations satisfying the in/out types
- analyse pipeline network constraints and decide warn/recommend single/network runners
- Indirect type handling in coder injection - this will enable using *KVBytes instead of KVBytes and less copy
- Exactly-Once processing as an extension to the existing at-least-once
- Develop general conception of stateful processing (parallel with guarantees and not just kv-stores but also prefix trees, etc.)

### Test Cases
 - Root committer works with backpressure, i.e. the longer the commits take the less frequent they become
 - Final commit is precise when limit is applied on a pipeline with unbounded root
 - Pipeline with network nodes fails if network runner is not used
 - Roots can select an exclusive node when running in a network 
 - sinks with timed triggers call flush on sinks
 - Sinks with no trigger will trigger once at the end
 - Sinks and Folds with .TriggerEach(1) will emit and flush on every element respectively
 - sinks without timed triggers still checkpoint correctly and on bounded/limit condition call flush once at the end
 - folds with timed trigger always emit values at correct intervals
 - pipelines with bounded root always terminate with correct result
 - filters, maps, flatmaps and sinks preserve order with Par(>1) 
 - Limits work on roots, filters, maps, flatmaps, folds, sinks with or without Par(>1) 
 - Limits work on stages that follow after a fold
 - Multiple folds works correctly, e.g. user fold fn followed by counter

## The Concept

The API is partially based on reflection but this is only used in-stream for user defined functions - implementation of transform interfaces use type casting in the worst case, no reflection. 
    
The the aim is to be able to inject coders and network stages behind the scenes so it looks something like this:

    A. pipeline is declared statically with only those transforms that are required logically for the job
    
    B. pipeline's distributed/parallel aspect is then analysed and network coders are injected accordingly
    
    C. pipeline type analysis is performed and type coders are injected accordingly
    
    D. final pipeline is materialized which starts all routines that wrap around the stage channels
    
    E. materialized pipeline is drained and if all sources are bounded it will eventually terminate
    

Example Pipeline based on the reflection api:
    
    
Declared pipeline:
    
    :  AMQP-QUEUE:[Bytes]                                                   (1) logical
    >> [Node]:XML-FILTER:[Node]                                             (2) logical
    >> [Bytes]:STD-OUT:[]                                                   (3) logical
                                                                            
Pipeline with injected network coders                                   
                                                                            
    :  AMQP-QUEUE:[Bytes]                                                   (1) logical
    >> [Bytes]:NET-SPLIT:[Bytes]                                             -  net-out
    >> [Node]:XML-FILTER:[Node]                                             (2) logical
    >> [Bytes]:NET-MERGE-SORT:[Bytes]                                        -  net-in
    >> [Bytes]:STD-OUT:[]                                                   (3) logical
                                                                               
Final Pipeline with injected type coders                                   
                                                                               
    :  AMQP-QUEUE:[Bytes]                                                   (1) logical
    >> [Bytes]:NET-SPLIT:[Bytes]                                             -  net-out
    >> [Bytes]:XML-DECODER:[Node]                                            -  decode
    >> [Node]:XML-FILTER:[Node]                                             (2) logical
    >> [Node]:XML-DECODER:[Bytes]                                            -  encode
    >> [Bytes]:NET-MERGE-SORT:[Bytes]                                        -  net-in
    >> [Bytes]:STD-OUT:[]                                                   (3) logical
    
    
Say 3 instances of this pipelines are executed then this is what has to happen in terms of network
    
    
(1) AMQP Source - can be executed symmetrically without coordination but only one of the instances will be active
 -  Net-Split - stamps the input elements and distributes them among the instances using round robin strategy 
(2) XML Filter is a pure map transform so can also be executed symmetrically and run in parallel without coordination
 -  Net-Merge-Sort - receives all the outputs and ensures the ordering per source instance
    - it runs only one node - which one is informed by the logical stage, for STD-OUT sink it wouuld make
      sense to run it on the one that joined the group last, as that's where the user will prefer to see the output
(3) STD-OUT - simply runs everywhere since it is the Net-Merge that controls where the output is

So there are several implications of this:
- the default mode of coordination is no coordination - the stage runs everywhere unless constrained otherwise
- Network coders are informed by their surrounding logical stages
- all records must be stamped to:
    a) preserve absolute ordering if required by a network merge stage
    b) provide a means for optimistic checkpointing 
- Network In Coders are informed by the succeeding stage but there are many subtleties to consider e.g.:
    - JMS SOURCE - runs on all and fan-out to all but has to use pessimistic checkpointing becuase in some implementations it is not possible to ack "up to a point"
    - AMQP QUEUE SOURCE - run on all and fan-out to all - it will have only one active instance by the protocol with optimistic checkpoitning as it support delivery tagging
    - KAFKA SOURCE - run on all with no coordination - kafka consumer is partitioned
    - LOCAL FILE SOURCE - runs on one specific node and does fan-out to all of the file contents  
    - REMOTE FILE SOURCE - will be divided into 2 sub-stages (either explicitly or implicitly)
        - a) list of URLs runs on any one node and fan-out to all
        - b) file contents is retrieved from - runs on all by default  
    - STD-OUT - runs on one specific node selected by the user    
       
     
     
     
    
    
    

