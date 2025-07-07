<?php

namespace Ontec\ReactRedisStreams;

use Clue\Redis\Protocol\Model\ErrorReply;
use Clue\Redis\Protocol\Model\ModelInterface;
use Clue\Redis\Protocol\Model\MultiBulkReply;
use Clue\Redis\Protocol\Parser\ParserException;
use Clue\Redis\Protocol\Parser\ParserInterface;
use Clue\Redis\Protocol\Serializer\SerializerInterface;
use Evenement\EventEmitter;
use Illuminate\Support\Arr;
use React\Stream\DuplexStreamInterface;

class StreamConnection extends EventEmitter implements ConnectionInterface
{
	protected bool $ending = false;
	protected bool $closed = false;
	protected bool $_running = false;
	protected bool $dropping = false;

	public function __construct(protected DuplexStreamInterface $io,
								protected ParserInterface       $parser,
								protected SerializerInterface   $serializer,
								protected StreamSettings        $settings) {
		$io->on('data', fn(mixed $data) => $this->data($data));
		$io->on('close', fn() => $this->close());
	}

	public function alive(): bool {
		return !$this->ending && !$this->closed &&
			$this->io->isReadable() && $this->io->isWritable();
	}

	public function running(): bool {
		return $this->_running;
	}

	/** Drop current blocking operation response. */
	public function drop(): static {
		$this->running() and $this->dropping = true; // Prevent operation on non-blocked stream.
		return $this;
	}

	public function run(): static {
		$this->idle();
		return $this;
	}

	public function end(): static {
		$this->ending = true;
		return $this;
	}

	public function close(): void {
		if(!$this->closed) {
			$this->ending = $this->closed = true;
			$this->_running = false;
			$this->io->close();
			$this->emit('close');
		}
	}

	protected function idle(): void {
		if($this->closed || $this->ending)
			$this->emit('error', [new ConnectionCloseException($this->ending, 'SOCKET_ENOTCONN')]);
		if(!$this->ending) {
			$this->_running = true;
			$this->settings->advanced() ? $this->xReadGroup() : $this->xRead();
		} else
			$this->close();
	}

	protected function arguments(): array {
		$args = [];
		$this->settings->limit and $args[] = ['COUNT', $this->settings->limit];
		$this->settings->timeout >= 0 and $args[] = ['BLOCK', $this->settings->timeout];
		$args[] = ['STREAMS', array_keys($this->settings->streams), array_values($this->settings->streams)];
		return $args;
	}

	protected function xRead(): void {
		if(!count($this->settings->streams))
			throw new \RangeException('No streams selected.');
		if($this->settings->timeout < 0)
			throw new \RuntimeException('Non-blocking loop prevention.');
		$args = Arr::flatten($this->arguments());
//		file_put_contents('debug.log', microtime(true).': XREAD '.implode(' ', $args).PHP_EOL, FILE_APPEND);
		$this->io->write($this->serializer->getRequestMessage('XREAD', $args));
	}

	protected function xReadGroup(): void {
		if(!count($this->settings->streams))
			throw new \RangeException('No streams selected.');
		if($this->settings->consumer === '' || $this->settings->group === '')
			throw new \UnexpectedValueException('No customer or group specified.');
		if($this->settings->timeout < 0)
			throw new \RuntimeException('Non-blocking loop prevention.');
		$args = Arr::flatten(['GROUP', $this->settings->group, $this->settings->consumer, $this->arguments()]);
//		file_put_contents('debug.log', microtime(true).': XREADGROUP '.implode(' ', $args).PHP_EOL, FILE_APPEND);
		$this->io->write($this->serializer->getRequestMessage('XREADGROUP', $args));
	}

	protected function data(mixed $data): void {
		try {
			$responses = $this->parser->pushIncoming($data);
		} catch(ParserException $error) {
			$this->emit('error', [new InvalidDataException($error)]);
			$this->close();
			return;
		}

		array_walk($responses, fn(ModelInterface $response) => $this->handle($response));
		// Idle call may be moved into future tick if the call stack still grows.
		if($this->ending) $this->close(); elseif($this->running()) $this->idle();
	}

	protected function handle(ModelInterface $response): void {
		if($this->dropping) { // Abort last blocking (read) operation.
			$this->dropping = false;
			return;
		}

		if(!($response instanceof ErrorReply)) {
			$data = $response->getValueNative();
			$data = !is_null($data) ? $this->structurize($data) : [];
			foreach($this->settings->streams as $name => $cursor) {
				$stream = $data[$name] ?? [];
				if($this->settings->advanced()) {
					if($cursor != '>')
						$this->settings->streams[$name] = $stream ? array_key_last($stream) : '>';
				} elseif($stream)
					$this->settings->streams[$name] = array_key_last($stream);
			}
			$this->emit('read', [$data, $this->settings->streams]);
		} else
			$this->emit('error', [$response]);
	}

	protected function structurize(iterable $data): array {
		$streams = [];
		foreach($data as $a) {
			$stream = [];
			foreach($a[1] as $b) {
				$record = [];
				$i = is_array($b[1]) ? new \ArrayIterator($b[1]) : new \IteratorIterator($b[1]);
				while($i->valid()) {
					$k = $i->current(); $i->next();
					$record[$k] = $i->current(); $i->next();
				}
				$stream[$b[0]] = $record;
			}
			$streams[$a[0]] = $stream;
		}
		return $streams;
	}
}
