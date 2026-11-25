package ingest

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// APIKeyUnaryInterceptor validates x-api-key gRPC metadata when apiKeys is configured.
// If apiKeys is empty, auth is bypassed (convenient for unauthenticated local dev).
func APIKeyUnaryInterceptor(apiKeys map[string]struct{}) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if len(apiKeys) == 0 {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		vals := md.Get("x-api-key")
		if len(vals) == 0 || vals[0] == "" {
			return nil, status.Error(codes.Unauthenticated, "missing x-api-key")
		}

		if _, valid := apiKeys[vals[0]]; !valid {
			return nil, status.Error(codes.PermissionDenied, "invalid x-api-key")
		}

		return handler(ctx, req)
	}
}

type hostLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
	rps      rate.Limit
	burst    int
}

func newHostLimiter(rps rate.Limit, burst int) *hostLimiter {
	return &hostLimiter{
		limiters: make(map[string]*rate.Limiter),
		rps:      rps,
		burst:    burst,
	}
}

func (h *hostLimiter) allow(hostID string) bool {
	h.mu.RLock()
	lim, ok := h.limiters[hostID]
	h.mu.RUnlock()

	if !ok {
		h.mu.Lock()
		lim, ok = h.limiters[hostID]
		if !ok {
			lim = rate.NewLimiter(h.rps, h.burst)
			h.limiters[hostID] = lim
		}
		h.mu.Unlock()
	}

	return lim.Allow()
}
