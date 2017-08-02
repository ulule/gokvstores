package gokvstores

import "time"

// Options are store options.
type Options struct {
	Expiration time.Duration
}

// Option is a functional option.
type Option func(*Options)

func newOptions(opts ...Option) Options {
	opt := Options{}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

// WithExpiration sets the expiration time.
func WithExpiration(d time.Duration) Option {
	return func(o *Options) {
		o.Expiration = d
	}
}
