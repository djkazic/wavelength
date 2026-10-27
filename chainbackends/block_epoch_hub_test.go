package chainbackends

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/lightninglabs/lndclient"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/stretchr/testify/require"
)

// epochTestTimeout bounds every wait in the block epoch hub tests.
const epochTestTimeout = 5 * time.Second

// fakeEpochStream is one lndclient block epoch stream opened by the hub.
type fakeEpochStream struct {
	//nolint:containedctx
	ctx     context.Context
	heights chan int32
	errs    chan error
}

// fakeEpochNotifier records every block epoch stream the hub opens.
type fakeEpochNotifier struct {
	lndclient.ChainNotifierClient

	mu      sync.Mutex
	streams []*fakeEpochStream

	// openErrs fails the next opens, one entry per open.
	openErrs []error

	// blockOpen makes opens wait for their context to be cancelled.
	blockOpen bool

	// inFlight counts opens that have not returned yet.
	inFlight atomic.Int32
}

// RegisterBlockEpochNtfn opens a controllable fake stream.
func (n *fakeEpochNotifier) RegisterBlockEpochNtfn(ctx context.Context) (
	chan int32, chan error, error) {

	n.inFlight.Add(1)
	defer n.inFlight.Add(-1)

	n.mu.Lock()
	stream := &fakeEpochStream{
		ctx:     ctx,
		heights: make(chan int32, 100),
		errs:    make(chan error, 1),
	}
	n.streams = append(n.streams, stream)

	var openErr error
	if len(n.openErrs) > 0 {
		openErr = n.openErrs[0]
		n.openErrs = n.openErrs[1:]
	}
	blockOpen := n.blockOpen
	n.mu.Unlock()

	if blockOpen {
		<-ctx.Done()

		return nil, nil, ctx.Err()
	}
	if openErr != nil {
		return nil, nil, openErr
	}

	return stream.heights, stream.errs, nil
}

// opens returns how many streams have been opened.
func (n *fakeEpochNotifier) opens() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return len(n.streams)
}

// stream waits for the idx-th opened stream.
func (n *fakeEpochNotifier) stream(t *testing.T, idx int) *fakeEpochStream {
	t.Helper()

	var stream *fakeEpochStream
	require.Eventually(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()

		if len(n.streams) <= idx {
			return false
		}
		stream = n.streams[idx]

		return true
	}, epochTestTimeout, time.Millisecond)

	return stream
}

// fakeHashKit resolves heights to deterministic hashes.
type fakeHashKit struct {
	lndclient.ChainKitClient

	calls atomic.Int32

	// failures fails this many lookups before succeeding.
	failures atomic.Int32
}

// GetBlockHash returns the deterministic hash for a height.
func (k *fakeHashKit) GetBlockHash(ctx context.Context, height int64) (
	chainhash.Hash, error) {

	k.calls.Add(1)

	if err := ctx.Err(); err != nil {
		return chainhash.Hash{}, err
	}
	if k.failures.Add(-1) >= 0 {
		return chainhash.Hash{}, errors.New("hash lookup failed")
	}

	return testEpochHash(int32(height)), nil
}

// testEpochHash is the hash fakeHashKit reports for a height.
func testEpochHash(height int32) chainhash.Hash {
	return chainhash.Hash{byte(height), byte(height >> 8), 0xbe}
}

// newEpochTestNotifier returns a notifier over the fakes. The notifier is
// stopped on cleanup.
func newEpochTestNotifier(t *testing.T) (*LndClientChainNotifier,
	*fakeEpochNotifier, *fakeHashKit) {

	t.Helper()

	notifier := &fakeEpochNotifier{}
	kit := &fakeHashKit{}
	n := NewLndClientChainNotifier(LndClientChainNotifierConfig{
		LND: &lndclient.LndServices{
			ChainNotifier: notifier,
			ChainKit:      kit,
		},
	})
	t.Cleanup(func() {
		require.NoError(t, n.Stop())
	})

	return n, notifier, kit
}

// register adds a block epoch registration that must succeed.
func register(t *testing.T,
	n *LndClientChainNotifier) *chainntnfs.BlockEpochEvent {

	t.Helper()

	event, err := n.RegisterBlockEpochNtfn(nil)
	require.NoError(t, err)

	return event
}

// recvEpoch waits for the next epoch and checks its height and hash.
func recvEpoch(t *testing.T, event *chainntnfs.BlockEpochEvent, height int32) {
	t.Helper()

	select {
	case epoch, ok := <-event.Epochs:
		require.True(t, ok, "epoch channel closed")
		require.Equal(t, height, epoch.Height)
		require.Equal(t, testEpochHash(height), *epoch.Hash)

	case <-time.After(epochTestTimeout):
		t.Fatalf("no epoch for height %d", height)
	}
}

// requireNoEpoch asserts that no epoch is pending on the registration.
func requireNoEpoch(t *testing.T, event *chainntnfs.BlockEpochEvent) {
	t.Helper()

	select {
	case epoch, ok := <-event.Epochs:
		if ok {
			t.Fatalf("unexpected epoch at height %d", epoch.Height)
		}
		t.Fatal("epoch channel closed unexpectedly")

	case <-time.After(50 * time.Millisecond):
	}
}

// requireClosed waits for the registration's channel to close, discarding
// any epochs still buffered.
func requireClosed(t *testing.T, event *chainntnfs.BlockEpochEvent) {
	t.Helper()

	timeout := time.After(epochTestTimeout)
	for {
		select {
		case _, ok := <-event.Epochs:
			if !ok {
				return
			}

		case <-timeout:
			t.Fatal("epoch channel was not closed")
		}
	}
}

// TestBlockEpochHubFanOut verifies that many registrations share one lnd
// stream and one hash lookup per height.
func TestBlockEpochHubFanOut(t *testing.T) {
	t.Parallel()

	n, notifier, kit := newEpochTestNotifier(t)

	events := make([]*chainntnfs.BlockEpochEvent, 0, 5)
	for range 5 {
		events = append(events, register(t, n))
	}
	require.Equal(t, 1, notifier.opens())

	notifier.stream(t, 0).heights <- 100
	for _, event := range events {
		recvEpoch(t, event, 100)
	}
	require.Equal(t, int32(1), kit.calls.Load())
}

// TestBlockEpochHubLateJoinerGetsTip verifies that a registration joining an
// open stream first receives the current tip, then live heights, without
// duplicates.
func TestBlockEpochHubLateJoinerGetsTip(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)

	early := register(t, n)
	stream := notifier.stream(t, 0)
	stream.heights <- 100
	recvEpoch(t, early, 100)

	late := register(t, n)
	recvEpoch(t, late, 100)

	stream.heights <- 101
	recvEpoch(t, early, 101)
	recvEpoch(t, late, 101)

	requireNoEpoch(t, early)
	requireNoEpoch(t, late)
	require.Equal(t, 1, notifier.opens())
}

// TestBlockEpochHubSlowSubscriber verifies that an unread registration
// neither blocks the others nor loses epochs.
func TestBlockEpochHubSlowSubscriber(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)

	slow := register(t, n)
	fast := register(t, n)
	stream := notifier.stream(t, 0)

	const numBlocks = 50
	for height := int32(1); height <= numBlocks; height++ {
		stream.heights <- height
		recvEpoch(t, fast, height)
	}

	for height := int32(1); height <= numBlocks; height++ {
		recvEpoch(t, slow, height)
	}
}

// TestBlockEpochHubStreamEnd verifies that every registration closes when
// the shared stream ends, and that a new registration opens a new stream.
func TestBlockEpochHubStreamEnd(t *testing.T) {
	t.Parallel()

	ends := map[string]func(*fakeEpochStream){
		"height channel closed": func(s *fakeEpochStream) {
			close(s.heights)
		},
		"stream error": func(s *fakeEpochStream) {
			s.errs <- errors.New("stream failed")
		},
	}

	for name, end := range ends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			n, notifier, _ := newEpochTestNotifier(t)

			first := register(t, n)
			second := register(t, n)

			end(notifier.stream(t, 0))
			requireClosed(t, first)
			requireClosed(t, second)

			again := register(t, n)
			require.Equal(t, 2, notifier.opens())

			notifier.stream(t, 1).heights <- 7
			recvEpoch(t, again, 7)
		})
	}
}

// TestBlockEpochHubCancel verifies that cancel is idempotent, that the last
// cancel tears the stream down, and that an immediate re-registration opens
// a fresh stream.
func TestBlockEpochHubCancel(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)

	first := register(t, n)
	second := register(t, n)
	stream := notifier.stream(t, 0)

	first.Cancel()
	first.Cancel()
	requireClosed(t, first)
	require.NoError(t, stream.ctx.Err(), "stream cancelled early")

	second.Cancel()
	requireClosed(t, second)
	require.Error(t, stream.ctx.Err(), "stream left open")

	again := register(t, n)
	require.Equal(t, 2, notifier.opens())

	notifier.stream(t, 1).heights <- 9
	recvEpoch(t, again, 9)
}

// TestBlockEpochHubOpenFailure verifies that a failed stream open is
// returned and the next registration retries the open.
func TestBlockEpochHubOpenFailure(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)

	openErr := errors.New("lnd unavailable")
	notifier.openErrs = []error{openErr}

	_, err := n.RegisterBlockEpochNtfn(nil)
	require.ErrorIs(t, err, openErr)

	event := register(t, n)
	require.Equal(t, 2, notifier.opens())

	notifier.stream(t, 1).heights <- 3
	recvEpoch(t, event, 3)
}

// TestBlockEpochHubLookupRetry verifies that a failed hash lookup is retried
// without a new block, and that a registration joining while the lookup is
// failing still receives the tip.
func TestBlockEpochHubLookupRetry(t *testing.T) {
	t.Parallel()

	n, notifier, kit := newEpochTestNotifier(t)
	kit.failures.Store(1)

	early := register(t, n)
	notifier.stream(t, 0).heights <- 100

	require.Eventually(t, func() bool {
		return kit.calls.Load() >= 1
	}, epochTestTimeout, time.Millisecond)
	late := register(t, n)

	recvEpoch(t, early, 100)
	recvEpoch(t, late, 100)
	requireNoEpoch(t, late)
}

// TestBlockEpochHubStop verifies that Stop closes every registration and
// rejects later ones.
func TestBlockEpochHubStop(t *testing.T) {
	t.Parallel()

	n, _, _ := newEpochTestNotifier(t)

	first := register(t, n)
	second := register(t, n)

	require.NoError(t, n.Stop())
	requireClosed(t, first)
	requireClosed(t, second)

	_, err := n.RegisterBlockEpochNtfn(nil)
	require.ErrorIs(t, err, errNotifierStopped)
}

// TestBlockEpochHubStopDuringOpen verifies that Stop cancels a hanging
// stream open, fails the opener and every registration waiting on it, and
// returns only after they resolved.
func TestBlockEpochHubStopDuringOpen(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)
	notifier.blockOpen = true

	const numRegs = 3
	errs := make(chan error, numRegs)
	for range numRegs {
		go func() {
			_, err := n.RegisterBlockEpochNtfn(nil)
			errs <- err
		}()
	}

	// Wait for the open to start so Stop has to cancel it.
	notifier.stream(t, 0)

	require.NoError(t, n.Stop())

	// Stop must not return while the cancelled lnd call is still running.
	require.Zero(t, notifier.inFlight.Load())

	for range numRegs {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, errNotifierStopped)

		case <-time.After(epochTestTimeout):
			t.Fatal("registration did not return after Stop")
		}
	}
	require.Equal(t, 1, notifier.opens())

	_, err := n.RegisterBlockEpochNtfn(nil)
	require.ErrorIs(t, err, errNotifierStopped)
}

// TestBlockEpochHubConcurrentChurn races registrations and cancellations
// against live heights and stream restarts. Run under -race it checks the
// ownership model for data races, panics, deadlocks, and leaked channels.
func TestBlockEpochHubConcurrentChurn(t *testing.T) {
	t.Parallel()

	n, notifier, _ := newEpochTestNotifier(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Feed heights to whichever stream is current, and periodically end
	// it so subscribers see stream restarts.
	var feeder sync.WaitGroup
	feeder.Add(1)
	go func() {
		defer feeder.Done()

		height := int32(0)
		for ctx.Err() == nil {
			notifier.mu.Lock()
			var stream *fakeEpochStream
			if last := len(notifier.streams) - 1; last >= 0 {
				stream = notifier.streams[last]
			}
			notifier.mu.Unlock()

			if stream != nil && stream.ctx.Err() == nil {
				height++
				select {
				case stream.heights <- height:
				default:
				}

				if height%25 == 0 {
					select {
					case stream.errs <- errors.New("churn"):
					default:
					}
				}
			}

			time.Sleep(time.Millisecond)
		}
	}()

	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()

			for range 50 {
				event, err := n.RegisterBlockEpochNtfn(nil)
				if err != nil {
					continue
				}

				// Read a little, then cancel and drain to
				// prove the channel is always closed.
				select {
				case <-event.Epochs:
				case <-time.After(2 * time.Millisecond):
				}

				event.Cancel()
				requireClosed(t, event)
			}
		}()
	}

	workers.Wait()
	cancel()
	feeder.Wait()
}
