<?php

namespace Ontec\ReactRedisStreams;

class StreamSettings
{
	public array $streams = [];
	public string $consumer = '';
	public string $group = '';
	public int $timeout = -1;
	public int $limit = 0;

	public function advanced(): bool {
		return $this->consumer !== '' && $this->group !== '';
	}
}