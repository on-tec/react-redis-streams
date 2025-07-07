<?php

namespace Ontec\ReactRedisStreams;

use Carbon\Carbon;
use Carbon\CarbonInterval;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use function \React\Promise\Timer\sleep as decay;
use function \React\Promise\resolve as resolved;

class DecayingValue extends PromisedValue
{
	protected CarbonInterval $ttl;
	protected CarbonInterval $threshold;
	protected ?PromiseInterface $decaying = null;
	protected ?Carbon $since = null;

	public function __construct(callable $resolver, CarbonInterval $ttl, protected LoopInterface $loop) {
		parent::__construct($resolver);
		$this->threshold = CarbonInterval::create();
		$this->ttl = $ttl->isEmpty() || $ttl->gt($this->threshold) ? $ttl : $this->threshold;
	}

	public function prolong(): static {
		if(!$this->ttl->isEmpty()) // Infinite TTL.
			if(!$this->decaying || $this->since->diff(Carbon::now(), true)->gt($this->threshold)) {
				$this->decaying?->cancel();
				$this->since = Carbon::now();
				$this->decaying = decay($this->ttl->totalSeconds, $this->loop)->then([$this, 'dispose']);
			}
		return $this;
	}

	public function throttle(?CarbonInterval $threshold): static {
		$this->threshold = $threshold ?? CarbonInterval::create();
		return $this;
	}

	protected function resolve(): PromiseInterface {
		$this->value = null;
		$this->decaying?->cancel();
		$resolving = resolved(($this->resolver)());
		$this->pending = $pending = new Deferred(function() use($resolving) {
			$resolving->cancel();
			$this->pending = $this->decaying = $this->value = null;
		});
		$resolving->then(function(mixed $value) use($pending) {
			$this->value = $value;
			$this->pending = null;
			$pending->resolve($value);
			$this->prolong();
		}, fn(\Throwable $e) => $pending->reject($e));
		return $pending->promise();
	}

	public function dispose(): static {
		$this->decaying?->cancel();
		if(!is_null($this->value))
			$this->disposer and ($this->disposer)($this->value);
		else
			$this->pending?->promise()->cancel();
		$this->pending = $this->decaying = $this->value = null;
		return $this;
	}
}