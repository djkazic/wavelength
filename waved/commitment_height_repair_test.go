package waved

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btclog/v2"
	"github.com/lightninglabs/wavelength/arkrpc"
	"github.com/lightninglabs/wavelength/db"
	"github.com/lightninglabs/wavelength/indexer"
	"github.com/lightninglabs/wavelength/internal/expiryfixture"
	mailboxrpc "github.com/lightninglabs/wavelength/mailbox/rpc"
	"github.com/lightninglabs/wavelength/vtxo"
	"github.com/lightningnetwork/lnd/clock"
	fn "github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestHasUnknownCommitmentHeight verifies that only usable ancestry with at
// least one missing height enters the startup repair.
func TestHasUnknownCommitmentHeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc *vtxo.Descriptor
		want bool
	}{
		{
			name: "nil descriptor",
		},
		{
			name: "empty ancestry",
			desc: &vtxo.Descriptor{},
		},
		{
			name: "all heights known",
			desc: &vtxo.Descriptor{
				Ancestry: []vtxo.Ancestry{{
					CommitmentHeight: 100,
				}},
			},
		},
		{
			name: "zero height",
			desc: &vtxo.Descriptor{
				Ancestry: []vtxo.Ancestry{
					{},
				},
			},
			want: true,
		},
		{
			name: "mixed heights",
			desc: &vtxo.Descriptor{
				Ancestry: []vtxo.Ancestry{
					{
						CommitmentHeight: 100,
					},
					{},
				},
			},
			want: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t, test.want,
				hasUnknownCommitmentHeight(test.desc),
			)
		})
	}
}

// commitmentRepairRPC injects transport failures into a real indexer client.
// The ancestry fixtures and database writes still use production validators.
type commitmentRepairRPC struct {
	recoveryIndexerStub
	requestMu sync.Mutex
	keys      []string
	onSend    func(context.Context, int) error
}

// SendRPC records every attempt, including rejected requests.
func (r *commitmentRepairRPC) SendRPC(ctx context.Context,
	method mailboxrpc.ServiceMethod, req proto.Message,
	opts mailboxrpc.RPCOptions) (mailboxrpc.SendResult, error) {

	r.requestMu.Lock()
	r.keys = append(r.keys, opts.IdempotencyKey)
	attempt := len(r.keys)
	r.requestMu.Unlock()
	if r.onSend != nil {
		if err := r.onSend(ctx, attempt); err != nil {
			return mailboxrpc.SendResult{}, err
		}
	}

	return r.recoveryIndexerStub.SendRPC(ctx, method, req, opts)
}

// requestKeys returns a snapshot safe to inspect while the worker is running.
func (r *commitmentRepairRPC) requestKeys() []string {
	r.requestMu.Lock()
	defer r.requestMu.Unlock()

	return append([]string(nil), r.keys...)
}

// newCommitmentRepairServer persists missing heights on signed ancestry and
// returns a daemon wired to the actual store and indexer query path.
func newCommitmentRepairServer(t *testing.T, count int, clk clock.Clock) (
	*Server, *commitmentRepairRPC, []*vtxo.Descriptor) {

	t.Helper()
	sqliteDB := db.NewTestDB(t)
	rpc := &commitmentRepairRPC{
		recoveryIndexerStub: recoveryIndexerStub{
			byScript: make(map[string]*arkrpc.VTXO),
		},
	}
	s := &Server{
		clk:         clk,
		log:         btclog.Disabled,
		daemonReady: make(chan struct{}),
		chainBackend: &heightOnlyChainBackend{
			height: 500,
		},
		proofKeyBackend: &recoveryKeyBackend{},
		vtxoStore: db.NewStore(
			sqliteDB.DB, sqliteDB.Queries, sqliteDB.Backend(),
			btclog.Disabled,
		).NewVTXOStore(clk),
		indexer: indexer.New(
			rpc, nil, "test-server", "client:test",
			fn.None[btclog.Logger](),
		),
	}
	var descs []*vtxo.Descriptor
	for i := range count {
		tag := byte(i + 1)
		_, pub := btcec.PrivKeyFromBytes([]byte{tag, 1})
		_, operator := btcec.PrivKeyFromBytes([]byte{tag, 2})
		script, err := txscript.PayToTaprootScript(
			txscript.ComputeTaprootKeyNoScript(pub),
		)
		require.NoError(t, err)
		candidate, _ := expiryfixture.Round(
			t, 10000, script, 50, 321, tag,
		)
		ancestry, err := vtxo.IndexedAncestryFromRPC(candidate)
		require.NoError(t, err)
		for j := range ancestry {
			ancestry[j].CommitmentHeight = 0
		}
		desc := &vtxo.Descriptor{
			Amount: btcutil.Amount(candidate.ValueSat),
			Outpoint: wire.OutPoint{
				Index: candidate.Outpoint.Vout,
			},
			PkScript: script,
			ClientKey: keychain.KeyDescriptor{
				PubKey: pub,
				KeyLocator: keychain.KeyLocator{
					Index: uint32(i),
				},
			},
			OperatorKey:    operator,
			Ancestry:       ancestry,
			CommitmentTxID: ancestry[0].CommitmentTxID,
			RoundID:        candidate.RoundId,
			CreatedHeight:  candidate.CreatedHeight,
			RelativeExpiry: 50,
			BatchExpiry:    1000,
			Status:         vtxo.VTXOStatusLive,
		}
		copy(desc.Outpoint.Hash[:], candidate.Outpoint.Txid)
		if i == count-1 {
			desc.Status = vtxo.VTXOStatusUnilateralExit
		}
		require.NoError(t, s.vtxoStore.SaveVTXO(t.Context(), desc))
		require.NoError(
			t,
			s.vtxoStore.UpdateVTXOStatus(
				t.Context(), desc.Outpoint, desc.Status,
			),
		)
		rpc.byScript[string(script)] = candidate
		descs = append(descs, desc)
	}

	return s, rpc, descs
}

// TestRepairLegacyCommitmentHeightsRetriesShedQueries proves an inventory
// larger than an operator burst completes without a restart. Both recoverable
// and exiting descriptors retain their proof and status across the backfill.
func TestRepairLegacyCommitmentHeightsRetriesShedQueries(t *testing.T) {
	t.Parallel()
	clk := clock.NewTestClock(time.Now())
	s, rpc, descs := newCommitmentRepairServer(t, 25, clk)
	var before []*vtxo.Descriptor
	for _, desc := range descs {
		reloaded, err := s.vtxoStore.GetVTXO(t.Context(), desc.Outpoint)
		require.NoError(t, err)
		before = append(before, reloaded)
	}
	rpc.onSend = func(_ context.Context, attempt int) error {
		if attempt == 21 || attempt == 22 {
			return status.Error(
				codes.ResourceExhausted, "rate limited",
			)
		}

		return nil
	}
	require.NoError(t, s.repairLegacyCommitmentHeights(t.Context()))
	keys := rpc.requestKeys()
	require.Len(t, keys, 27)
	require.NotEmpty(t, keys[20])
	require.Equal(t, keys[20], keys[21])
	require.Equal(t, keys[21], keys[22])
	require.NotEqual(t, keys[19], keys[20])
	require.NotEqual(t, keys[22], keys[23])
	for i, desc := range descs {
		reloaded, err := s.vtxoStore.GetVTXO(t.Context(), desc.Outpoint)
		require.NoError(t, err)
		require.Equal(
			t, int32(321), reloaded.Ancestry[0].CommitmentHeight,
		)
		reloaded.Ancestry[0].CommitmentHeight = 0
		require.Equal(t, before[i].Ancestry, reloaded.Ancestry)
		require.Equal(t, desc.Status, reloaded.Status)
	}
	// Already repaired inventory must not consume the shared query budget.
	require.NoError(t, s.repairLegacyCommitmentHeights(t.Context()))
	require.Len(t, rpc.requestKeys(), len(keys))
}

// TestRepairLegacyCommitmentHeightsCancellationPreservesProgress proves a
// cancelled pass stops querying immediately and the next pass picks up only
// the unfinished inventory, using the heights already committed to storage.
func TestRepairLegacyCommitmentHeightsCancellationPreservesProgress(
	t *testing.T) {

	t.Parallel()
	clk := clock.NewTestClock(time.Now())
	s, rpc, descs := newCommitmentRepairServer(t, 6, clk)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rpc.onSend = func(_ context.Context, attempt int) error {
		if attempt == 4 {
			cancel()

			return ctx.Err()
		}

		return nil
	}
	err := s.repairLegacyCommitmentHeights(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "3 of 6")
	require.Len(t, rpc.requestKeys(), 4)
	var repaired int
	for _, desc := range descs {
		reloaded, err := s.vtxoStore.GetVTXO(t.Context(), desc.Outpoint)
		require.NoError(t, err)
		if !hasUnknownCommitmentHeight(reloaded) {
			repaired++
		}
	}
	require.Equal(t, 3, repaired)
	require.NoError(t, s.repairLegacyCommitmentHeights(t.Context()))
	require.Len(t, rpc.requestKeys(), 7)
	for _, desc := range descs {
		reloaded, err := s.vtxoStore.GetVTXO(t.Context(), desc.Outpoint)
		require.NoError(t, err)
		require.False(t, hasUnknownCommitmentHeight(reloaded))
	}
}

// commitmentRepairLog delivers the maintenance summary to the test without
// racing a bytes.Buffer read against the background logger.
type commitmentRepairLog chan string

// Write forwards a log record to a buffered test channel.
func (l commitmentRepairLog) Write(p []byte) (int, error) {
	l <- string(p)

	return len(p), nil
}

// TestLegacyCommitmentHeightRepairResumesPartialPass proves a failed target is
// retried after the cooldown while completed targets are skipped. Full
// readiness is independent of repair completion and the worker exits on
// success.
func TestLegacyCommitmentHeightRepairResumesPartialPass(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		failures int
		err      error
	}{
		{
			"deadline",
			1,
			context.DeadlineExceeded,
		},
		{"rate limit retry exhaustion", recoveryIndexerRetryAttempts,
			status.Error(codes.ResourceExhausted, "rate limited")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ticks := make(chan time.Duration, 1)
			clk := clock.NewTestClockWithTickSignal(
				time.Now(), ticks,
			)
			s, rpc, descs := newCommitmentRepairServer(t, 3, clk)
			logs := make(commitmentRepairLog, 10)
			s.log = btclog.NewSLogger(
				btclog.NewDefaultHandler(logs),
			)
			rpc.onSend = func(_ context.Context, n int) error {
				if n <= test.failures {
					return test.err
				}

				return nil
			}
			stop := s.startLegacyCommitmentHeightRepair(t.Context())
			t.Cleanup(stop)
			require.Empty(t, rpc.requestKeys())
			s.markDaemonReady()
			select {
			case delay := <-ticks:
				require.Equal(
					t, legacyCommitmentHeightRepairInterval,
					delay,
				)

			case <-time.After(
				legacyCommitmentHeightRepairTimeout +
					5*time.Second,
			):
				t.Fatal("repair did not schedule the next pass")
			}
			require.Len(t, rpc.requestKeys(), 2+test.failures)
			var repaired int
			for _, desc := range descs {
				reloaded, err := s.vtxoStore.GetVTXO(
					t.Context(), desc.Outpoint,
				)
				require.NoError(t, err)
				if !hasUnknownCommitmentHeight(reloaded) {
					repaired++
				}
			}
			require.Equal(t, 2, repaired)
			interval := legacyCommitmentHeightRepairInterval
			clk.SetTime(clk.Now().Add(interval))
			deadline := time.After(5 * time.Second)
			for {
				select {
				case record := <-logs:
					if !strings.Contains(
						record, "repair complete",
					) {

						continue
					}
					stop()
					require.Len(
						t, rpc.requestKeys(),
						3+test.failures,
					)
					assertHeightRepairComplete(t, s, descs)

					return

				case <-deadline:
					t.Fatal("repair never completed")
				}
			}
		})
	}
}

// TestLegacyCommitmentHeightRepairShutdown joins maintenance before resource
// teardown, whether shutdown arrives before readiness, in an RPC, or in
// backoff.
func TestLegacyCommitmentHeightRepairShutdown(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"startup", "query", "backoff"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			ticks := make(chan time.Duration, 1)
			clk := clock.NewTestClockWithTickSignal(
				time.Now(), ticks,
			)
			s, rpc, _ := newCommitmentRepairServer(t, 2, clk)
			entered := make(chan struct{})
			rpc.onSend = func(ctx context.Context, _ int) error {
				if phase == "backoff" {
					return status.Error(
						codes.Unavailable, "offline",
					)
				}
				close(entered)
				<-ctx.Done()

				return ctx.Err()
			}
			stop := s.startLegacyCommitmentHeightRepair(t.Context())
			t.Cleanup(stop)
			if phase != "startup" {
				s.markDaemonReady()
				if phase == "backoff" {
					select {
					case <-ticks:
					case <-time.After(5 * time.Second):
						t.Fatal(
							"no repair cooldown",
						)
					}
				} else {
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("repair did not query")
					}
				}
			}
			stopped := make(chan struct{})
			go func() {
				stop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("repair did not stop")
			}
			if phase == "startup" {
				require.Empty(t, rpc.requestKeys())
			}
			if phase == "query" {
				require.Len(t, rpc.requestKeys(), 1)
			}
		})
	}
}

// assertHeightRepairComplete checks the durable inventory after maintenance.
func assertHeightRepairComplete(t *testing.T, s *Server,
	descs []*vtxo.Descriptor) {

	t.Helper()
	for _, desc := range descs {
		reloaded, err := s.vtxoStore.GetVTXO(t.Context(), desc.Outpoint)
		require.NoError(t, err)
		require.False(t, hasUnknownCommitmentHeight(reloaded))
	}
}
