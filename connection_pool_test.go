package pool_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-home-iot/connection-pool"
	"github.com/stretchr/testify/require"
)

type mockConn struct {
	net.Conn
	CloseCalled func(*mockConn)
}

func (c *mockConn) Close() error {
	if c.CloseCalled != nil {
		c.CloseCalled(c)
	}
	return nil
}

func TestInitCreatesConnections(t *testing.T) {

	initCount := 0
	p := pool.NewPool(pool.Config{
		Size: 5,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			initCount++
			return &mockConn{}, nil
		},
	})

	// Init and wait for completion
	done := p.Init()
	<-done

	require.Equal(t, p.Config.Size, initCount)
}

func TestPoolCloseClosesAllConnections(t *testing.T) {
	newCount := 0
	closeCount := 0
	p := pool.NewPool(pool.Config{
		Size: 5,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			newCount++
			return &mockConn{
				CloseCalled: func(c *mockConn) {
					closeCount++
				},
			}, nil
		},
	})

	done := p.Init()
	<-done

	// The pool should not try to open connections again when it closes
	// them when it shuts down
	newCount = 0
	closed := p.Close()
	<-closed

	require.Equal(t, 0, newCount)
	require.Equal(t, p.Config.Size, closeCount)
}

func TestGetReturnsConnectionsAndErrsOnTimeout(t *testing.T) {
	p := pool.NewPool(pool.Config{
		Size: 3,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			return &mockConn{}, nil
		},
	})

	done := p.Init()
	<-done

	// Should be able to call Get 3 times, then timeout and get error on 4th
	for i := 0; i < p.Config.Size; i++ {
		c, err := p.Get(time.Second, false)
		require.Nil(t, err)
		require.NotNil(t, c)
	}

	start := time.Now()
	c, err := p.Get(time.Millisecond, false)
	end := time.Now()

	require.Nil(t, c)
	require.NotNil(t, err)
	require.Equal(t, err, pool.ErrTimeout)
	require.True(t, end.Sub(start) >= time.Millisecond)
}

func TestCloseReturnsTheConnectionToThePool(t *testing.T) {
	p := pool.NewPool(pool.Config{
		Size: 1,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			return &mockConn{}, nil
		},
	})

	done := p.Init()
	<-done

	c1, err := p.Get(time.Millisecond, false)
	require.NotNil(t, c1)
	require.Nil(t, err)

	// Second call should have run out of connections
	c, err := p.Get(time.Millisecond, false)
	require.Equal(t, err, pool.ErrTimeout)
	require.Nil(t, c)

	p.Release(c1)
	c2, err := p.Get(time.Millisecond, false)
	require.NotNil(t, c2)
	require.Nil(t, err)
	require.Equal(t, c1, c2)
}

func TestBadConnectionNotReturnedToThePool(t *testing.T) {
	newCalled := false
	p := pool.NewPool(pool.Config{
		Size: 1,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			newCalled = true
			return &mockConn{}, nil
		},
	})

	done := p.Init()
	<-done

	c1, err := p.Get(time.Millisecond, false)
	require.NotNil(t, c1)
	require.Nil(t, err)

	newCalled = false
	c1.IsBad = true
	p.Release(c1)

	c2, err := p.Get(time.Millisecond*100, false)
	require.NotNil(t, c2)
	require.Nil(t, err)
	require.NotEqual(t, c1, c2)
	require.True(t, newCalled)
}

func TestPoolKeepsTryingToOpenConnectionUntilSuccess(t *testing.T) {
	newCount := 0
	p := pool.NewPool(pool.Config{
		Size: 1,
		NewConnection: func(cfg pool.Config) (net.Conn, error) {
			newCount++

			// Simulate failing 5 times
			if newCount < 5 {
				return nil, errors.New("bad conn")
			}
			return &mockConn{}, nil
		},
	})

	done := p.Init()
	<-done

	require.Equal(t, 5, newCount)
}
