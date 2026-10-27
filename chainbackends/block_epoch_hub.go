package chainbackends

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/chainntnfs"
)

const (
	// blockEpochSubBufferSize is the capacity of each subscriber's output
	// channel. Epochs beyond it wait in the subscriber's unbounded
	// pending queue rather than blocking the shared stream.
	blockEpochSubBufferSize = 10

	// blockHashRetryBackoff is the initial delay before retrying a failed
	// block hash lookup for the latest reported height.
	blockHashRetryBackoff = time.Second

	// blockHashMaxRetryBackoff caps the block hash lookup retry delay.
	blockHashMaxRetryBackoff = 30 * time.Second
)

// errNotifierStopped is returned by block epoch registrations made after
// the notifier was stopped, including registrations that were waiting on a
// stream open that Stop cancelled.
var errNotifierStopped = errors.New("chain notifier stopped")

// blockEpochHub multiplexes one lndclient block epoch stream across every
// block epoch registration of a notifier.
//
// Without it, each registration opened its own gRPC stream to lnd and
// resolved the hash of every height with its own ChainKit call, so a client
// with hundreds of subscribers (one per live VTXO actor, plus finality
// watches) made hundreds of identical RPCs per block. The hub opens a single
// stream on demand, resolves each height once, and fans the epoch out
// in-process.
//
// Ownership rules that keep teardown race-free:
//   - Subscribers belong to the upstream generation they joined. An upstream
//     only ever touches its own subscriber map, which is detached (set to
//     nil) under mu when that generation ends, so an old generation can
//     never close a newer generation's subscribers.
//   - Enqueueing to a subscriber never blocks: it appends to an unbounded
//     queue. A wedged consumer therefore cannot stall delivery to the
//     others, and no epoch is dropped. The backlog lnd used to hold in its
//     per-client queue is now held here instead.
//   - Each subscriber's output channel is written and closed only by that
//     subscriber's worker goroutine.
//   - mu is never held across a blocking call. In particular the stream
//     open runs unlocked behind an explicit opening state that Stop can
//     cancel.
type blockEpochHub struct {
	lnd *lndclient.LndServices
	log btclog.Logger

	mu sync.Mutex

	// stopped is set by Stop. No registration succeeds afterwards.
	stopped bool

	// up is the currently open upstream stream, or nil.
	up *epochUpstream

	// opening is the in-flight stream open, or nil. Registrations that
	// find it set wait for it instead of opening a second stream.
	opening *epochOpen

	// nextID numbers subscribers for their generation's map.
	nextID uint64

	// wg tracks upstream goroutines, subscriber workers, and in-flight
	// stream opens and their waiters.
	wg sync.WaitGroup
}

// epochOpen is one in-flight attempt to open the upstream stream.
type epochOpen struct {
	// ctx becomes the upstream's context if the open succeeds.
	//
	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc

	// done is closed once the attempt resolved. err is set before.
	done chan struct{}
	err  error
}

// epochUpstream is one generation of the shared lndclient stream.
type epochUpstream struct {
	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc

	// The fields below are guarded by blockEpochHub.mu.

	// subs holds this generation's subscribers. It is nil once the
	// generation has been detached from the hub.
	subs map[uint64]*epochSub

	// lastHeight is the latest height lnd reported, valid when
	// haveHeight is set.
	lastHeight int32
	haveHeight bool

	// lastEpoch is the resolved epoch for lastHeight, or nil while its
	// hash lookup is outstanding. New subscribers receive it first,
	// mirroring lnd sending the current tip on registration.
	lastEpoch *chainntnfs.BlockEpoch
}

// epochSub is a single registration's delivery queue.
type epochSub struct {
	mu      sync.Mutex
	pending []*chainntnfs.BlockEpoch
	closed  bool

	// wake signals the worker that pending gained an entry.
	wake chan struct{}

	// quit is closed by stop to end the worker.
	quit     chan struct{}
	stopOnce sync.Once

	// out is the registration's Epochs channel. Only the worker writes
	// to and closes it.
	out chan *chainntnfs.BlockEpoch
}

// newBlockEpochHub returns a hub that opens its stream through lnd.
func newBlockEpochHub(lnd *lndclient.LndServices,
	log btclog.Logger) *blockEpochHub {

	return &blockEpochHub{
		lnd: lnd,
		log: log,
	}
}

// register adds a subscriber, opening the shared stream if none is open.
func (h *blockEpochHub) register() (*chainntnfs.BlockEpochEvent, error) {
	for {
		h.mu.Lock()
		if h.stopped {
			h.mu.Unlock()

			return nil, errNotifierStopped
		}

		// Join the open stream.
		if h.up != nil {
			event := h.addSubLocked(h.up)
			h.mu.Unlock()

			return event, nil
		}

		// Another registration is opening the stream: wait for it and
		// then join the result.
		if h.opening != nil {
			// The wait is tracked in wg so Stop returns only
			// after every waiter it released has resumed.
			attempt := h.opening
			h.wg.Add(1)
			h.mu.Unlock()

			<-attempt.done
			h.wg.Done()

			if attempt.err != nil {
				return nil, attempt.err
			}

			continue
		}

		// Open the stream ourselves, without holding mu, so Stop can
		// cancel a slow open instead of waiting behind it. The open is
		// tracked in wg so Stop also waits for the lndclient call to
		// return.
		ctx, cancel := context.WithCancel(context.Background())
		attempt := &epochOpen{
			ctx:    ctx,
			cancel: cancel,
			done:   make(chan struct{}),
		}
		h.opening = attempt
		h.wg.Add(1)
		h.mu.Unlock()

		return h.open(attempt)
	}
}

// open performs a stream open attempt published as h.opening and resolves
// it, either installing a new upstream generation or failing every waiter.
func (h *blockEpochHub) open(attempt *epochOpen) (*chainntnfs.BlockEpochEvent,
	error) {

	defer h.wg.Done()

	heightChan, errChan, err := h.lnd.ChainNotifier.RegisterBlockEpochNtfn(
		attempt.ctx,
	)

	h.mu.Lock()
	h.opening = nil

	// A stop wins over whatever the open returned: cancelling the open
	// usually surfaces as a gRPC Canceled error, and a stream that opened
	// anyway must not outlive Stop.
	if h.stopped || attempt.ctx.Err() != nil {
		err = errNotifierStopped
	}

	if err != nil {
		attempt.err = err
		close(attempt.done)
		h.mu.Unlock()

		// Cancelling also tears down a stream that opened after Stop.
		attempt.cancel()

		return nil, err
	}

	up := &epochUpstream{
		ctx:    attempt.ctx,
		cancel: attempt.cancel,
		subs:   make(map[uint64]*epochSub),
	}
	h.up = up

	h.wg.Add(1)
	go h.runUpstream(up, heightChan, errChan)

	event := h.addSubLocked(up)
	close(attempt.done)
	h.mu.Unlock()

	h.log.InfoS(up.ctx, "Opened shared block epoch stream")

	return event, nil
}

// addSubLocked creates a subscriber in the given generation and returns its
// registration. The caller must hold mu.
func (h *blockEpochHub) addSubLocked(
	up *epochUpstream) *chainntnfs.BlockEpochEvent {

	sub := &epochSub{
		wake: make(chan struct{}, 1),
		quit: make(chan struct{}),
		out: make(
			chan *chainntnfs.BlockEpoch, blockEpochSubBufferSize,
		),
	}

	id := h.nextID
	h.nextID++
	up.subs[id] = sub

	h.wg.Add(1)
	go sub.run(&h.wg)

	// The cached tip is enqueued under the same lock that publish holds
	// while caching an epoch and snapshotting subscribers, so a
	// subscriber gets each epoch exactly once.
	if up.lastEpoch != nil {
		sub.enqueue(up.lastEpoch)
	}

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			h.removeSub(up, id, sub)
		})
	}

	return &chainntnfs.BlockEpochEvent{
		Epochs: sub.out,
		Cancel: cancel,
	}
}

// removeSub cancels one registration. Removing the last subscriber detaches
// and cancels the generation before any later registration can join it.
func (h *blockEpochHub) removeSub(up *epochUpstream, id uint64, sub *epochSub) {
	var cancelUp context.CancelFunc

	h.mu.Lock()
	if up.subs != nil {
		delete(up.subs, id)

		if len(up.subs) == 0 && h.up == up {
			h.up = nil
			up.subs = nil
			cancelUp = up.cancel
		}
	}
	h.mu.Unlock()

	sub.stop()

	if cancelUp != nil {
		cancelUp()
	}
}

// runUpstream reads one generation's stream, resolves each height's hash
// once, and fans the epoch out. When the stream ends it closes every
// subscriber of the generation; their owners reconnect, which opens a new
// generation on demand.
func (h *blockEpochHub) runUpstream(up *epochUpstream, heightChan chan int32,
	errChan chan error) {

	defer h.wg.Done()
	defer h.endUpstream(up)

	ctx := up.ctx

	var (
		retry      *time.Timer
		retryC     <-chan time.Time
		backoff    = blockHashRetryBackoff
		tipHeight  int32
		stopRetrys = func() {
			if retry != nil {
				retry.Stop()
				retry, retryC = nil, nil
			}
		}
	)
	defer stopRetrys()

	// resolve looks up the tip's hash and publishes it. A failed lookup
	// is retried with backoff until it succeeds or a newer height
	// replaces it, so subscribers (including late joiners) always
	// converge on the current tip without waiting for the next block.
	resolve := func() {
		chainKit := h.lnd.ChainKit
		blockHash, err := chainKit.GetBlockHash(ctx, int64(tipHeight))
		if err != nil {
			if isBlockEpochShutdownError(ctx, err) {
				h.log.DebugS(ctx, "Block hash lookup cancelled "+
					"during stream shutdown",
					slog.Any("err", err),
					slog.Int("height", int(tipHeight)),
				)

				return
			}

			h.log.WarnS(ctx, "Failed to get block hash",
				err,
				slog.Int("height", int(tipHeight)),
				slog.Duration("retry_in", backoff),
			)

			retry = time.NewTimer(backoff)
			retryC = retry.C
			backoff = min(backoff*2, blockHashMaxRetryBackoff)

			return
		}

		backoff = blockHashRetryBackoff
		h.publish(up, &chainntnfs.BlockEpoch{
			Height: tipHeight,
			Hash:   &blockHash,
		})
	}

	for {
		select {
		case height, ok := <-heightChan:
			if !ok {
				h.log.InfoS(ctx, "Height channel closed")

				return
			}

			h.log.InfoS(ctx, "Received height from lndclient",
				slog.Int("height", int(height)),
			)

			// A newer height supersedes any pending retry.
			stopRetrys()
			backoff = blockHashRetryBackoff
			tipHeight = height

			h.mu.Lock()
			up.lastHeight = height
			up.haveHeight = true
			up.lastEpoch = nil
			h.mu.Unlock()

			resolve()

		case <-retryC:
			retry, retryC = nil, nil
			resolve()

		case err, ok := <-errChan:
			if ok && err != nil {
				if isBlockEpochShutdownError(ctx, err) {
					h.log.DebugS(ctx, "Block epoch "+
						"subscription cancelled",
						slog.Any("err", err),
					)
				} else {
					h.log.WarnS(ctx, "Block epoch error",
						err,
					)
				}
			}

			return

		case <-ctx.Done():
			return
		}
	}
}

// publish caches a resolved epoch as the generation's tip and enqueues it to
// every current subscriber. An epoch for a height that has since been
// superseded is dropped.
func (h *blockEpochHub) publish(up *epochUpstream,
	epoch *chainntnfs.BlockEpoch) {

	h.mu.Lock()
	if !up.haveHeight || up.lastHeight != epoch.Height {
		h.mu.Unlock()

		return
	}

	up.lastEpoch = epoch

	subs := make([]*epochSub, 0, len(up.subs))
	for _, sub := range up.subs {
		subs = append(subs, sub)
	}
	h.mu.Unlock()

	for _, sub := range subs {
		sub.enqueue(epoch)
	}
}

// endUpstream detaches a finished generation and closes its subscribers.
func (h *blockEpochHub) endUpstream(up *epochUpstream) {
	h.mu.Lock()
	if h.up == up {
		h.up = nil
	}
	subs := up.subs
	up.subs = nil
	h.mu.Unlock()

	up.cancel()

	for _, sub := range subs {
		sub.stop()
	}

	h.log.InfoS(up.ctx, "Shared block epoch stream closed",
		slog.Int("subscribers", len(subs)),
	)
}

// stop cancels any in-flight open, closes the open stream and all of its
// registrations, and waits for every hub goroutine to exit. Registrations
// made afterwards fail with errNotifierStopped.
func (h *blockEpochHub) stop() {
	h.mu.Lock()
	h.stopped = true

	// Cancel an in-flight open. The opener and its waiters are tracked
	// in wg, so the Wait below also covers them.
	if h.opening != nil {
		h.opening.cancel()
	}

	up := h.up
	h.up = nil

	var subs map[uint64]*epochSub
	if up != nil {
		subs = up.subs
		up.subs = nil
	}
	h.mu.Unlock()

	if up != nil {
		up.cancel()
	}
	for _, sub := range subs {
		sub.stop()
	}

	h.wg.Wait()
}

// enqueue appends an epoch to the subscriber's queue. It never blocks and is
// a no-op once the subscriber is stopped. Each subscriber gets its own copy
// so no consumer can observe another's mutation.
func (s *epochSub) enqueue(epoch *chainntnfs.BlockEpoch) {
	hash := *epoch.Hash
	cp := &chainntnfs.BlockEpoch{
		Height:      epoch.Height,
		Hash:        &hash,
		BlockHeader: epoch.BlockHeader,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return
	}
	s.pending = append(s.pending, cp)
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// stop prevents further enqueues and ends the worker, which closes out.
func (s *epochSub) stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.pending = nil
		s.mu.Unlock()

		close(s.quit)
	})
}

// run delivers queued epochs to out in order until stopped, then closes out.
func (s *epochSub) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(s.out)

	for {
		s.mu.Lock()
		if len(s.pending) == 0 {
			s.mu.Unlock()

			select {
			case <-s.wake:
				continue

			case <-s.quit:
				return
			}
		}

		epoch := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		s.mu.Unlock()

		select {
		case s.out <- epoch:
		case <-s.quit:
			return
		}
	}
}
