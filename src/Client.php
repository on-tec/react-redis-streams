<?php

namespace Ontec\ReactRedisStreams;

use Carbon\Carbon;
use Carbon\CarbonInterval;
use Clue\Redis\Protocol\Model\ErrorReply;
use Clue\Redis\Protocol\Parser\ResponseParser;
use Clue\Redis\Protocol\Serializer\RecursiveSerializer;
use Evenement\EventEmitter;
use Illuminate\Support\Arr;
use React\EventLoop\TimerInterface;
use React\Promise\PromiseInterface;
use React\Socket\Connector;
use React\Stream\DuplexStreamInterface;
use function React\Promise\resolve;
use function React\Promise\reject;

class Client extends EventEmitter
{
	protected RedisURL $url;
	protected PromisedValue $control;
	protected PromisedValue $streams;
	protected StreamSettings $settings;
	protected ?TimerInterface $_keepAlive = null;
	protected bool $ending = false;
	protected bool $closed = false;

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
					->on('error', fn(\Throwable $e) => $this->emit('error', [$e]))
					->on('close', fn() => $this->keepAlive());
			})))->validator(fn(StreamConnection $c) => $c->alive())
			->disposer(function(StreamConnection $c) { $c->removeAllListeners(); $c->close(); });
	}

	public function listen(array $streams): PromiseInterface {
		if($this->settings->streams != $streams && !$this->ending) {
			$this->settings->streams = $streams;
			$this->streams->dispose(); // Abort current blocking operation on streams connection and stop it.
			$this->keepAlive(); // Force connect if connection is not exists yet (otherwise it auto-reconnects).
		}
		if($this->ending)
			return reject(new ConnectionCloseException(!$this->closed, 'SOCKET_ENOTCONN'));
		elseif($this->settings->hasStreams())
			return $this->streams()->then(fn() => $this);
		else
			return resolve($this);
	}

	/**
	 * Used for:
	 * 1. XREAD/XREADGROUP regular interruption.
	 * 2. Streams connection alive check interval.
	 * @param CarbonInterval|null $value
	 * @return $this
	 */
	public function timeout(?CarbonInterval $value): static {
		$timeout = $value ? $value->totalMilliseconds : 0;
		if($this->settings->timeout != $timeout) {
			empty($this->_keepAlive) or $this->loop->cancelTimer($this->_keepAlive);
			$this->_keepAlive = empty($timeout) ? null : $this->loop
				->addPeriodicTimer($timeout / 1000, fn() => $this->keepAlive());
		}
		$this->settings->timeout = $timeout;
		return $this;
	}

	/**
	 * Used for:
	 * 1. XREAD/XREADGROUP results partitioning.
	 * 2. XPENDING entries pagination.
	 * 3. XTRIM limit per call.
	 * @param int|null $value
	 * @return $this
	 */
	public function limit(?int $value): static {
		$this->settings->limit = $value ?: 0;
		return $this;
	}

	/**
	 * Enables auto-trimming feature.
	 * @param mixed $depth Time interval or entries amount.
	 * @return $this
	 */
	public function trim(mixed $depth): static {
		if($depth instanceof CarbonInterval)
			$this->settings->trimBefore = round($depth->totalMilliseconds);
		elseif(is_numeric($depth))
			$this->settings->trimLength = intval($depth);
		else
			throw new \InvalidArgumentException('Depth must be either length or an interval.');
		return $this;
	}

	/**
	 * Sets consumer & group and enables shared reading mode.
	 * @param string|null $consumer
	 * @param string|null $group
	 * @return $this
	 */
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

	public function acknowledge(Entry $entry): PromiseInterface {
		$entry->id === '' and throw new \InvalidArgumentException('Entity id is required.');
		$entry->stream === '' and throw new \InvalidArgumentException('Stream is required.');
		$this->settings->scoped() or throw new \UnexpectedValueException('No customer or group specified.');
		return $this->xack($entry->stream, $this->group(), $entry->id);
	}

	public function __call(string $name, array $args): PromiseInterface {
		if(!$this->ending)
			return $this->control()->then(function(ControlConnection $c) use($name, $args) {
				$this->control->prolong();
				return $c->__call($name, $args)
					->finally(fn() => $this->control->prolong());
			});
		else
			return reject(new ConnectionCloseException(!$this->closed, 'SOCKET_ENOTCONN'));
	}

	public function end() {
		$this->ending = true;
		$this->control->existent(false)->then(fn(?ConnectionInterface $c) => $c?->end());
		$this->streams->existent(false)->then(fn(?ConnectionInterface $c) => $c?->end());
	}

	public function close() { // FIXME Not called automatically when inner connections get closed by end() call.
		if($this->closed)
			return;

		$this->ending = $this->closed = true;
		empty($this->_keepAlive) or $this->loop->cancelTimer($this->_keepAlive);
		$this->control->dispose();
		$this->streams->dispose();

		$this->emit('close');
		$this->removeAllListeners();
	}

	protected function keepAlive(): void {
		if($this->settings->hasStreams() && !$this->ending)
			$this->streams();
	}

	protected function connect(): PromiseInterface {
		return (new Connector(['timeout' => $this->url->timeout], $this->loop))->connect($this->url->getSocketURL())
			->then(fn(DuplexStreamInterface $connection) => new ControlConnection($connection, new ResponseParser(), new RecursiveSerializer()))
			->then(function(ControlConnection $connection) {
				if($credentials = $this->url->getCredentials())
					return $connection->auth($credentials[1], $credentials[0])
						->then(fn() => $connection, function(\Throwable $e) use($connection) {
							$connection->close();
							throw new AuthenticationException($e->getMessage());
						});
				else
					return resolve($connection);
			})->then(function(ControlConnection $connection) {
				if(($db = $this->url->database) >= 0)
					return $connection->select($db)
						->then(fn() => $connection, function(\Throwable $e) use($connection) {
							$connection->close();
							throw str_starts_with(ltrim($e->getMessage()), 'ERR DB')
								? new \OutOfBoundsException($e->getMessage()) : $e;
						});
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
