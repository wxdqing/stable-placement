package protoactor

import (
	"context"
	"errors"
	"fmt"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/remote"
	"github.com/asynkron/protoactor-go/service/cluster"
)

type Context struct {
	cluster  *cluster.Cluster
	resolver PIDResolver
}

const DefaultGrainCallRetryCount = 1

var _ cluster.Context = (*Context)(nil)

func NewContext(resolver PIDResolver) cluster.ContextProducer {
	return func(c *cluster.Cluster) cluster.Context { return &Context{cluster: c, resolver: resolver} }
}

func (c *Context) Request(placementContext *cluster.PlacementContext, identity, kind string, message interface{}, opts ...cluster.GrainCallOption) (interface{}, error) {
	config := c.callConfig(opts)
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	clusterIdentity := cluster.NewClusterIdentity(identity, kind)
	for attempt := 0; attempt < config.RetryCount; attempt++ {
		route, err := c.resolver.ResolvePID(ctx, placementContext, clusterIdentity)
		if err != nil {
			return nil, err
		}
		response, err := config.Context.RequestFuture(route.PID, message, config.Timeout).Result()
		if err == nil || response != nil {
			return response, err
		}
		if !retryableActorError(err) {
			return nil, err
		}
		c.resolver.Remove(clusterIdentity, route.PID)
		config.RetryAction(attempt)
	}
	return nil, fmt.Errorf("have reached max retries: %d", config.RetryCount)
}

func (c *Context) RequestFuture(placementContext *cluster.PlacementContext, identity, kind string, message interface{}, opts ...cluster.GrainCallOption) (actor.Future, error) {
	config := c.callConfig(opts)
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	route, err := c.resolver.ResolvePID(ctx, placementContext, cluster.NewClusterIdentity(identity, kind))
	if err != nil {
		return nil, err
	}
	return config.Context.RequestFuture(route.PID, message, config.Timeout), nil
}

func (c *Context) Send(placementContext *cluster.PlacementContext, identity, kind string, message interface{}, opts ...cluster.GrainCallOption) error {
	config := c.callConfig(opts)
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	route, err := c.resolver.ResolvePID(ctx, placementContext, cluster.NewClusterIdentity(identity, kind))
	if err != nil {
		return err
	}
	config.Context.Send(route.PID, message)
	return nil
}

func (c *Context) callConfig(opts []cluster.GrainCallOption) *cluster.GrainCallConfig {
	config := cluster.NewGrainCallOptions(c.cluster)
	for _, option := range opts {
		option(config)
	}
	if config.RetryCount <= 0 {
		config.RetryCount = DefaultGrainCallRetryCount
	}
	return config
}

func retryableActorError(err error) bool {
	return errors.Is(err, actor.ErrTimeout) || errors.Is(err, actor.ErrDeadLetter) ||
		errors.Is(err, remote.ErrTimeout) || errors.Is(err, remote.ErrDeadLetter)
}
