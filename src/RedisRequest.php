<?php

namespace Ontec\ReactRedisStreams;

use React\Promise\Deferred;
use React\Promise\PromiseInterface;

class RedisRequest
{
	public readonly string $command;
	public readonly ?array $arguments;
	protected Deferred $deferred;

	public function __construct(string $command, ?array $arguments = null) {
		$this->command = strtoupper($command);
		$this->arguments = $arguments;
		$this->deferred = new Deferred();
	}

	public function promise(): PromiseInterface {
		return $this->deferred->promise();
	}

	public function resolve($value): static {
		$this->deferred->resolve($value);
		return $this;
	}

	public function reject(\Throwable $reason): static {
		$this->deferred->reject($reason);
		return $this;
	}
}