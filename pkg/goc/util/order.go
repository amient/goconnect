package util

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/network/prototype"
)

func NewOrderedElementSet(cap int) *OrderedElementSet {
	return &OrderedElementSet{
		elements: make(map[uint64]*goc.Element, cap),
	}
}

type OrderedElementSet struct {
	next     uint64
	elements map[uint64]*goc.Element
}

func (set *OrderedElementSet) AddElement(elementToAdd *goc.Element, collector *prototype.Collector) {
	set.elements[elementToAdd.Stamp.Hi] = elementToAdd
	for ; set.elements[set.next + 1] != nil; {
		set.next ++
		collector.Emit(set.elements[set.next])
		delete(set.elements, set.next)
	}
}

//
//func NewOrderBuf(cap int) *OrderBuf {
//	return &OrderBuf{
//		pending: make(map[uint64]*goc.Checkpoint, cap),
//	}
//}
//
//type OrderBuf struct {
//	pending map[uint64]*goc.Checkpoint
//}
//
//
//
//func collapse() {
//		//resolve acks <> pending
//		for ; pending.valid() && pendingCheckpoints[pending.Lo] != nil && pendingCheckpoints[pending.Lo].acked; {
//			lo := pending.Lo
//			pending.Lo += 1
//			chk := pendingCheckpoints[lo]
//			delete(pendingCheckpoints, lo)
//			checkpoint[chk.Part] = chk.Data
//			//stream.log("STAGE[%d] DELETING PART %d DATA %v \n", stream.stage, chk.Part, chk.Data)
//		}
//		if len(pendingCheckpoints) < stream.cap && pendingSuspendable == nil {
//			//release the backpressure after capacity is freed
//			pendingSuspendable = stream.pending
//		}
//
//		//commit if pending commit request or not commitable in which case commit requests are never fired
//		if pendingCommitReuqest || !isCommitable {
//			doCommit()
//		}
//		maybeTerminate()
//	}
//
//}
