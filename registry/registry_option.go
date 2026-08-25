package registry

import "github.com/cschleiden/go-workflows/interceptor"

type RegisterOption interface {
	applyRegisterOption(registerConfig) registerConfig
}

type registerOptions []RegisterOption

func (opts registerOptions) applyRegisterOptions(cfg registerConfig) registerConfig {
	for _, opt := range opts {
		cfg = opt.applyRegisterOption(cfg)
	}
	return cfg
}

type registerOptionFunc func(registerConfig) registerConfig

func (f registerOptionFunc) applyRegisterOption(cfg registerConfig) registerConfig {
	return f(cfg)
}

// WithName sets a custom name for the registered workflow or activity.
func WithName(name string) RegisterOption {
	return registerOptionFunc(func(cfg registerConfig) registerConfig {
		cfg.Name = name
		return cfg
	})
}

// WithWorkflowInterceptors attaches interceptors that apply only to this specific workflow.
// These run after global interceptors in the chain.
func WithWorkflowInterceptors(interceptors ...interceptor.WorkflowInterceptor) RegisterOption {
	return registerOptionFunc(func(cfg registerConfig) registerConfig {
		cfg.WorkflowInterceptors = append(cfg.WorkflowInterceptors, interceptors...)
		return cfg
	})
}

// WithActivityInterceptors attaches interceptors that apply only to this specific activity.
// These run after global interceptors in the chain.
func WithActivityInterceptors(interceptors ...interceptor.ActivityInterceptor) RegisterOption {
	return registerOptionFunc(func(cfg registerConfig) registerConfig {
		cfg.ActivityInterceptors = append(cfg.ActivityInterceptors, interceptors...)
		return cfg
	})
}
