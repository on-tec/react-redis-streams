<?php

namespace Ontec\ReactRedisStreams;

use Carbon\Carbon;
use Carbon\CarbonInterval;
use Clue\Redis\Protocol\Model\ErrorReply;
use Clue\Redis\Protocol\Parser\ResponseParser;
use Clue\Redis\Protocol\Serializer\RecursiveSerializer;
use Evenement\EventEmitter;
use Illuminate\Support\Arr;
use React\Promise\PromiseInterface;
use React\Socket\Connector;
use React\Stream\DuplexStreamInterface;
use function React\Promise\resolve;

class Client extends EventEmitter
{
	protected RedisURL $url;
	protected PromisedValue $control;
	protected PromisedValue $streams;
	protected StreamSettings $settings;

	public function __construct(string $url, protected \React\EventLoop\LoopInterface $loop) {
		$this->url = new RedisURL($url);
		$this->control = (new DecayingValue(fn() =>
			$this->connect()->then(fn(ControlConnection $c) =>
				$c->on('error', fn(\Throwable $e) => $this->emit('error', [$e]))),
			CarbonInterval::seconds($this->url->decay), $this->loop))
			->throttle(CarbonInterval::seconds(1))
			->validator(fn(ControlConnection $c) => $c->alive())
			->disposer(function(ControlConnection $c) { $c->removeAllListeners(); $c->close(); });

		$this->settings = new StreamSettings();
		$this->streams = (new PromisedValue(fn() =>
			$this->connect()->then(function(ControlConnection $c) {
				$property = new \ReflectionProperty($c::class, 'io');
				$s = tap($property->getValue($c), fn(DuplexStreamInterface $s) => $s->removeAllListeners());
				$c = new StreamConnection($s, new ResponseParser(), new RecursiveSerializer(), $this->settings);
				return $c->on('read', fn(mixed $data) => $this->emit('read', [$data]))
					->on('fail', fn(mixed $info) => $this->emit('fail', [$info]))
					->on('error', fn(\Throwable $e) => $this->emit('error', [$e]));
			})))->validator(fn(StreamConnection $c) => $c->alive())
			->disposer(function(StreamConnection $c) { $c->removeAllListeners(); $c->close(); });
	}

	public function stream(string $name, string $cursor): static {
		if(($this->settings->streams[$name] ?? null) != $cursor) {
			$this->settings->streams[$name] = $cursor;
			$this->streams->existent()->then(fn(?StreamConnection $c) => $c?->invalidate());
		}
		return $this;
	}

	public function timeout(?CarbonInterval $value): static {
		$this->settings->timeout = $value ? $value->totalMilliseconds : 0;
		return $this;
	}

	public function limit(?int $value): static {
		$this->settings->limit = $value ?: 0;
		return $this;
	}

	public function trim(mixed $threshold): static {
		if($threshold instanceof CarbonInterval)
			$this->settings->trimBefore = round($threshold->totalMilliseconds);
		elseif(is_numeric($threshold))
			$this->settings->trimLength = intval($threshold);
		else
			throw new \InvalidArgumentException('Threshold must be either length or an interval.');
		return $this;
	}

	public function scope(?string $consumer = null, ?string $group = null): static {
		if(!$consumer || !$group)
			$consumer = $group = '';
		if($this->settings->group != $group || $this->settings->consumer != $consumer) {
			$this->streams->unresolved() or throw ReconfigurationException::running();
			$this->settings->group = $group;
			$this->settings->consumer = $consumer;
		}
		return $this;
	}

	public function consumer(): ?string {
		return $this->settings->consumer ?: null;
	}

	public function group(): ?string {
		return $this->settings->group ?: null;
	}

	/**
	 * @param CarbonInterval $after Maximum duration of entry processing time.
	 * @param int $times Maximum retries before entry gets considered unprocessable.
	 * @param int $every Maximum new entries before check for stalled ones.
	 * @return $this
	 */
	public function retry(CarbonInterval $after, int $times, int $every = 0, bool $foreign = false): static {
		$this->settings->maxRetries = $times;
		$changed = $this->settings->retryAfter != $after->totalMilliseconds
			|| $this->settings->retryEvery != $every
			|| $this->settings->retryForeign != $foreign;
		if($changed) {
			$this->streams->unresolved() or throw ReconfigurationException::running();
			$this->settings->retryAfter = $after->totalMilliseconds;
			$this->settings->retryEvery = $every;
			$this->settings->retryForeign = $foreign;
		}
		return $this;
	}

	public function run(): PromiseInterface {
		return $this->streams()->then(function(StreamConnection $c) {
			$c->run();
			return $this;
		});
	}

	/** Abort current blocking operation on streams connection and stop it. */
	public function reset(): static {
		$this->streams->dispose();
		return $this;
	}

	public function record(Entry $entry, bool $existing = false): PromiseInterface {
		empty($entry->stream) and throw new \InvalidArgumentException('Stream is required.');
		$entry->count() > 0 or throw new \InvalidArgumentException('Entry must have at least one field.');
		$args = [$entry->stream];
		$existing and $args[] = 'NOMKSTREAM';
		if($this->settings->trimable()) {
			// TODO ACKED (v8.2.0)
			$args[] = $this->settings->trimLength > 0
				? ['MAXLEN', '~', $this->settings->trimLength]
				: ['MINID', '~', Carbon::now('UTC')->subMilliseconds($this->settings->trimBefore)->getTimestampMs()];
			$this->settings->limited() and $args[] = ['LIMIT', $this->settings->limit];
		}
		$args[] = $entry->id !== '' ? $entry->id : '*';
		foreach($entry as $key => $value)
			$args[] = [$key, strval($value)];
		return $this->xadd(...Arr::flatten($args));
	}

	public function acknowledge(array|string|int $id, string $stream): PromiseInterface {
		$ids = is_string($id) || is_numeric($id) ? [$id] : $id;
		$this->settings->scoped() or throw new \UnexpectedValueException('No customer or group specified.');
		return $this->xack($stream, $this->group(), ...$ids);
	}

	public function __call(string $name, array $args) {
		return $this->control()->then(function(ControlConnection $c) use($name, $args) {
			$this->control->prolong();
			return $c->__call($name, $args)
				->finally(fn() => $this->control->prolong());
		});
	}

	public function end() {
		$this->control->existent(false)->then(fn(?ConnectionInterface $c) => $c?->end());
		$this->streams->existent(false)->then(fn(?ConnectionInterface $c) => $c?->end());
	}

	public function close() {
		$this->control->dispose();
		$this->streams->dispose();
		$this->emit('close');
		$this->removeAllListeners();
	}

	protected function connect(): PromiseInterface {
		return (new Connector(['timeout' => $this->url->timeout], $this->loop))->connect($this->url->getSocketURL())
			->then(fn(DuplexStreamInterface $connection) => new ControlConnection($connection, new ResponseParser(), new RecursiveSerializer()))
			->then(function(ControlConnection $connection) {
				if($credentials = $this->url->getCredentials())
					return $connection->auth($credentials[1], $credentials[0])->then(fn() => $connection,
						fn(\Throwable $e) => throw new AuthenticationException($e->getMessage()));
				else
					return resolve($connection);
			})->then(function(ControlConnection $connection) {
				if(($db = $this->url->database) >= 0)
					return $connection->select($db)->then(fn() => $connection, fn(\Throwable $e) =>
						throw str_starts_with(ltrim($e->getMessage()), 'ERR DB') ?
							new \OutOfBoundsException($e->getMessage()) : $e);
				else
					return resolve($connection);
			});
	}

	protected function control(): PromiseInterface { // Fixes __call() overlap.
		return $this->control->__invoke();
	}
	protected function streams(): PromiseInterface { // Fixes __call() overlap.
		return $this->streams->__invoke();
	}
}
