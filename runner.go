package kafka

type Runner struct {
	forwarder *Forwarder
}

func (r *Runner) Start() {
}

func (r *Runner) Stop() {
	r.forwarder.Close()
}
