package goc

type Checkpoint interface{}

//drain-resume is used at the moment before we figure out the right model for checkpoint associations beteween stages
//once that is solved, Drain will still be used for pessimistic checkpointing
//pessimistic checkpointing means any one of the stages that requires checkpointing doesn't support
//any sort of watermark or offset - it still may support ack / commit but that is not enough
//to provide reliable general guarantees without blocking, e.g. some JMS client implementations
