<?php

namespace Ontec\ReactRedisStreams;

use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use function \React\Promise\resolve as resolved;

class PromisedValue
{
	protected ?Deferred $pending = null;
	protected mixed $value = null;
	protected \Closure $resolver;
	protected ?\Closure $validator;
	protected ?\Closure $disposer;

	public function __construct(callable $resolver) {
		$this->resolver = $resolver(...);
	}

	public function __invoke(): PromiseInterface {
		if(is_null($this->value))
			return $this->pending?->promise() ?? $this->resolve();
		elseif($this->validate($this->value)) // Value is valid.
			return resolved($this->value);
		else // Value is not valid, recreate.
			return $this->dispose()->resolve();
	}

	public function existent(bool $validated = true): PromiseInterface {
		if(!is_null($this->value))
			return resolved(!$validated || $this->validate($this->value) ? $this->value : null);
		else
			return $this->pending?->promise() ?? resolved(null);
	}

	public function unresolved(): bool {
		return is_null($this->value) ? is_null($this->pending) : !$this->validate($this->value);
	}

	public function validator(callable $fn): static {
		$this->validator = $fn(...);
		return $this;
	}

	public function disposer(callable $fn): static {
		$this->disposer = $fn(...);
		return $this;
	}

	protected function resolve(): PromiseInterface {
		$this->value = null;
		$resolving = resolved(($this->resolver)());
		$this->pending = $pending = new Deferred(function() use($resolving) {
			$resolving->cancel();
			$this->pending = $this->value = null;
		});
		$resolving->then(function(mixed $value) use($pending) {
			$this->value = $value;
			$this->pending = null;
			$pending->resolve($value);
		}, fn(\Throwable $e) => $pending->reject($e));
		return $pending->promise();
	}

	protected function validate(mixed $value): bool {
		return !$this->validator || ($this->validator)($value);
	}

	public function dispose(): static {
		if(!is_null($this->value))
			$this->disposer and ($this->disposer)($this->value);
		else
			$this->pending?->promise()->cancel();
		$this->pending = $this->value = null;
		return $this;
	}
}