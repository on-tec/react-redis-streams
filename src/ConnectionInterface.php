<?php

namespace Ontec\ReactRedisStreams;

interface ConnectionInterface
{
	public function alive(): bool;
	public function end(): static;
	public function close(): void;
}