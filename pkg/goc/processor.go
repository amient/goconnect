/*
 * Copyright 2018 Amient Ltd, London
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package goc

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc/util"
	"math"
	"sync"
)

/**
 * A Processor is an element-wise function that can be parallelised vertically with prserved ordering.
 * It is a fundamental runtime concept which also implements buffering, limiting and termination conditions.
 */

type Work struct {
	id      uint64
	element *Element
	out     chan WorkResult
}

func (w *Work) Emit(e *Element) {
	w.out <- WorkResult{w.id, w.element.Stamp.Uniq, e}
}

type WorkResult struct {
	id            uint64
	upstreamStamp uint64
	data          *Element
}

func NewWorkerGroup(c *Context, p Processor) *WorkerGroup {
	return &WorkerGroup{
		p: p,
		c: c,
	}
}

type WorkerGroup struct {
	p    Processor
	c    *Context
	acks chan uint64
}

func (g *WorkerGroup) Start(input chan *Element) *WorkerGroup {

	lim := g.c.def.limit
	par := g.c.def.maxVerticalParallelism

	results := make(chan WorkResult, g.c.def.bufferCap*par)

	work := make(chan Work, 32)
	go func() {
		defer close(work)
		stamp := uint64(0)
		for in := range input {
			stamp++
			work <- Work{stamp, in, results}
		}
	}()

	//the work is done by multiple goroutines competing for the work input channel
	group := &sync.WaitGroup{}
	for i := 0; i < par; i ++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			fn := g.p.Materialize()
			for w := range work {
				w.element.ack = g.c.up.ack
				fn(w.element, &w)
				results <- WorkResult{w.id, w.element.Stamp.Uniq, nil}
			}
		}(i)
	}
	//separate goroutine watches for eos on the input work and then closes the (unordered) results
	go func() {
		group.Wait()
		close(results)
	}()

	tracingAcks := g.c.ack == nil
	var pendingUp map[uint64]*int     //upstream stamp counter of pending acks
	var pendingDown map[uint64]uint64 //downstream stamp -> upstream stamp
	var acks chan uint64
	var stops chan uint64
	if tracingAcks {
		pendingUp = make(map[uint64]*int)
		pendingDown = make(map[uint64]uint64)
		g.c.log("TRACING ACKS WITH BUFFER SIZE: %d",  g.c.def.bufferCap)
		acks = make(chan uint64, g.c.def.bufferCap)
		stops = make(chan uint64, 1)
		g.c.ack = func(uniq uint64) {
			acks <- uniq
		}
		g.c.stop = func(uniq uint64) {
			stops <- uniq
		}
	}

	go func() {
		defer close(g.c.output)
		defer g.c.close()
		limitEnabled := lim > 0
		//g.c.log("LIMIT=%d, PAR=%d, LIMIT ENABLED=%v", lim, par, limitEnabled)

		var stopStamp uint64 = math.MaxUint64
		var doStop = func(uniq uint64) {
			stopStamp = uniq
			if ! tracingAcks {
				g.c.up.stop(stopStamp)
			} else {
				upstreamStopStamp := pendingDown[uniq]
				//discount all bigger downstream stamps pointing to the same upstream stamp
				//this is here for correct termination conditions when limit is applied on stages that have 1-to-many outputs
				for pd := range pendingDown {
					if pd > stopStamp {
						up := pendingDown[pd]
						unacked := pendingUp[up]
						delete(pendingDown, pd)
						*unacked --
					}
				}
				g.c.up.stop(upstreamStopStamp)
			}
		}

		counter := uint64(0)
		doOutput := func(result WorkResult) {
			counter++
			e := result.data
			if e.Stamp.Uniq == 0 {
				e.Stamp.Uniq = counter
			}
			//g.c.log("doOutput: %d -> %d", e.Stamp.Uniq, result.upstreamStamp)
			if limitEnabled {
				if counter == lim {
					if g.c.stop == nil {
						panic(fmt.Errorf("stop function is nil"))
					} else {
						g.c.log("Limit Reached: Stamp =%v, Limt=%v, Counter=%v ", e.Stamp.Uniq, lim, counter)
						g.c.stop(e.Stamp.Uniq)
					}
				} else if counter > lim {
					return
				}
			}

			if tracingAcks {
				var unacked *int
				var ok bool
				if unacked, ok = pendingUp[result.upstreamStamp]; ! ok {
					i := 0
					unacked = &i
					pendingUp[result.upstreamStamp] = unacked
				}
				*unacked++
				pendingDown[e.Stamp.Uniq] = result.upstreamStamp
			}
			e.ack = g.c.ack
			g.c.output <- e
		}

		terminating := false
		terminated := false
		maybeTerminate := func() {
			if terminating {
				terminated = true
				if tracingAcks {
					for pd := range pendingDown {
						if pd <= stopStamp {
							terminated = false
							break
						}
					}
				}
			}
		}

		next := uint64(1)
		outputCache := make(map[uint64][]WorkResult, 100)
		//moving average for learning what capacity should be given to the output buffers
		movingAvgBufferSize := util.NewMovingAverage(7)
		var c float64
		for !terminated {
			select {
			case uniq := <-stops:
				doStop(uniq)
				maybeTerminate()
			case uniq, _ := <-acks:
				if uniq <= stopStamp {
					upstreamStamp := pendingDown[uniq]
					delete(pendingDown, uniq)
					unacked := pendingUp[upstreamStamp]
					*unacked--
					//g.c.log("[%d] UPSTREAM STAMP[%d] UNACKED : %d", uniq, upstreamStamp, *unacked)
					if *unacked == 0 || uniq > stopStamp {
						delete(pendingUp, upstreamStamp)
						g.c.up.ack(upstreamStamp)
						maybeTerminate()
					}
				}

			case result, ok := <-results:
				if !ok {
					results = nil
					terminating = true
					maybeTerminate()
				} else if par == 1 {
					if result.data != nil {
						doOutput(result)
					}
				} else {
					if result.id == next {
						if result.data != nil {
							c = c + 1
							doOutput(result)
						} else {
							movingAvgBufferSize.Add(c)
							c = 0
							next++
							done := false
							for !done {
								var b []WorkResult
								if b, done = outputCache[next]; done {
									delete(outputCache, next)
									for _, c := range b {
										if c.data != nil {
											movingAvgBufferSize.Add(float64(len(b)))
											doOutput(c)
										} else {
											done = false
											next++
										}
									}
								} else {
									done = true
								}
							}
						}
					} else {
						if outputCache[result.id] == nil {
							outputCache[result.id] = make([]WorkResult, 0, int(math.Max(1, movingAvgBufferSize.Avg())))
						}
						outputCache[result.id] = append(outputCache[result.id], result)
					}
				}

			}
		}
		if len(outputCache) > 0 {
			panic(fmt.Errorf("final output cache after termination: %v", outputCache))
		}
	}()

	return g
}

type MapProcessor struct {
	fn  Mapper
}

func (p *MapProcessor) Materialize() func(input *Element, context PContext) {
	f := p.fn.Materialize()
	return func(input *Element, ctx PContext) {
		ctx.Emit(&Element{
			FromNodeId: input.FromNodeId,
			Stamp:      input.Stamp,
			Value:      f(input.Value),
		})
	}
}


type FilterProcessor struct {
	fn Filter
}

func (p *FilterProcessor) Materialize() func(input *Element, context PContext) {
	f := p.fn.Materialize()
	return func(input *Element, ctx PContext) {
		if f(input.Value) {
			ctx.Emit(input)
		} else {
			input.Ack()
		}
	}
}
