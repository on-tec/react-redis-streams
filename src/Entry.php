<?php

namespace Ontec\ReactRedisStreams;

use Carbon\Carbon;
use Illuminate\Contracts\Support\Arrayable;
use Traversable;

class Entry implements \ArrayAccess, \Countable, \IteratorAggregate, \JsonSerializable, Arrayable
{
	public string $id = '';
	public int $retries = -1;
	public string $consumer = '';
	public ?Carbon $claimed_at = null;
	public string $stream = '';
	protected array $data = [];

	/**
	 * @param string $id (optional)
	 * @param array $data (optional)
	 * @param string $stream (optional)
	 */
	public function __construct(...$args) {
		if(count($args) > 0)
			if(is_string($args[0]) || is_integer($args[0])) {
				$this->id = strval($args[0]);
				array_shift($args);
			} elseif(is_null($args[0]))
				array_shift($args);

		if(count($args) > 0)
			if(is_array($args[0])) {
				$this->data = $args[0];
				array_shift($args);
			} elseif(is_null($args[0]))
				array_shift($args);

		if(count($args) > 0)
			if(is_string($args[0])) {
				$this->stream = $args[0];
				array_shift($args);
			}

		if(count($args) > 0)
			throw new \InvalidArgumentException('Entry id, data and stream name were expected.');
	}

	public function offsetExists(mixed $offset): bool {
		return isset($this->data[$offset]);
	}

	public function offsetGet(mixed $offset): mixed {
		return $this->data[$offset] ?? null;
	}

	public function offsetSet(mixed $offset, mixed $value): void {
		is_null($offset) and throw new \OutOfBoundsException('Only simple field => value map is supported.');
		is_string($offset) or throw new \UnexpectedValueException('Field name must be a string.');
		$this->data[$offset] = $value;
	}

	public function offsetUnset(mixed $offset): void {
		unset($this->data[$offset]);
	}

	public function count(): int {
		return count($this->data);
	}

	public function getIterator(): Traversable {
		return new \ArrayIterator($this->data);
	}

	public function jsonSerialize(): mixed {
		return $this->data;
	}

	public function toArray(): array {
		return $this->data;
	}

	public function toLog(): array {
		$entry = [];
		$this->id !== '' and $entry['id'] = $this->id;
		$this->stream !== '' and $entry['stream'] = $this->stream;
		$this->consumer !== '' and $entry['consumer'] = $this->consumer;
		$this->retries >= 0 and $entry['retries'] = $this->retries;
		is_null($this->claimed_at) or $entry['claimed_at'] = strval($this->claimed_at);
		$this->isEmpty() or $entry['payload'] = $this->data;
		return $entry;
	}

	public function isEmpty(): bool {
		return !count($this->data);
	}

	public function isRetry(): bool {
		return $this->retries >= 1;
	}
}