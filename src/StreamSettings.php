<?php

namespace Ontec\ReactRedisStreams;

class StreamSettings
{
	public array $streams = [];
	public string $consumer = '';
	public string $group = '';
	public int $timeout = -1; // In milliseconds. Endless if 0. Disabled if -1.
	public int $limit = 0; // Disabled if 0.
	public int $retryAfter = 0; // In milliseconds. Disabled if 0.
	public int $retryEvery = 0; // Disabled if 0.
	public int $maxRetries = 0; // Endless if 0.
	public bool $retryForeign = false;

	public function hasStreams(): int {
		return count($this->streams);
	}

	public function scoped(): bool {
		return $this->consumer !== '' && $this->group !== '';
	}

	public function blocking(): bool {
		return $this->timeout >= 0;
	}

	public function endlessBlocking(): bool {
		return $this->timeout == 0;
	}

	public function limited(): bool {
		return $this->limit > 0;
	}

	public function durable(): bool {
		return $this->retryAfter > 0;
	}

	public function chunkedRetry(): bool {
		return $this->retryEvery > 0;
	}

	public function failableRetry(): bool {
		return $this->maxRetries > 0;
	}
}