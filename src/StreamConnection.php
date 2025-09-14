<?php

namespace Ontec\ReactRedisStreams;

use Carbon\Carbon;
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
	protected \SplQueue $queue;
	protected bool $ending = false;
	protected bool $closed = false;
	protected bool $_running = false;
	protected bool $blocking = false;
	protected bool $dropping = false;
	protected ?Carbon $lastRetry = null;
	protected int $leftEntries = -1;

	public function __construct(protected DuplexStreamInterface $io,
								protected ParserInterface       $parser,
								protected SerializerInterface   $serializer,
								protected StreamSettings        $settings) {
		$this->queue = new \SplQueue();
		$io->on('data', fn(mixed $data) => $this->data($data));
		$io->on('close', fn() => $this->close());
		$this->lastRetry = $this->settings->durable() ? Carbon::now() : null; // Use cursor = 0 for first pending iteration.
	}

	public function alive(): bool {
		return !$this->ending && !$this->closed &&
			$this->io->isReadable() && $this->io->isWritable();
	}

	public function running(): bool {
		return $this->_running;
	}

	/** Cursor or streams changed => drop current read state & blocking operation response. */
	public function invalidate(): static {
		if($this->running()) {
			$this->lastRetry = $this->settings->durable() ? Carbon::now() : null;
			$this->leftEntries = -1; // Reset or disable.
			if($this->blocking)
				$this->dropping = true;
		}
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
			$this->settings->scoped() ? $this->doReadGroup() : $this->xRead();
		} else
			$this->close();
	}

	protected function xRead(): void {
		$this->settings->hasStreams() or throw new \RangeException('No streams selected.');
		$this->settings->blocking() or throw new \RuntimeException('Non-blocking loop prevention.');
		$args = [];
		$this->settings->limit and $args[] = ['COUNT', $this->settings->limit];
		$this->settings->timeout >= 0 and $args[] = ['BLOCK', $this->settings->timeout];
		$args[] = ['STREAMS', array_keys($this->settings->streams), array_values($this->settings->streams)];
		$this->execute(new RedisRequest('XREAD', Arr::flatten($args)));
	}

	protected function doReadGroup(): void {
		$this->settings->hasStreams() or throw new \RangeException('No streams selected.');
		$this->settings->scoped() or throw new \UnexpectedValueException('No customer or group specified.');
		$this->settings->blocking() or throw new \RuntimeException('Non-blocking loop prevention.');

		if($this->settings->durable()) {
			list($timeToRetry, $countToRetry) = [$this->timeToRetry(), $this->countToRetry()];
			#debug_log("Time left: {$timeToRetry}; count left: {$countToRetry}");
			if($timeToRetry > 0 && $countToRetry != 0) {
				$limit = $countToRetry > 0 ? min($this->settings->limit, $countToRetry) : $this->settings->limit;
				$this->xReadGroup($limit, min($timeToRetry + 1, $this->settings->timeout));
			} else {
				$this->settings->limited() or throw new \UnexpectedValueException('Entity limit is required for retrospection.');
				$this->lastRetry = Carbon::now();
				$this->leftEntries = $this->settings->chunkedRetry() ? $this->settings->retryEvery : -1;
				foreach($this->settings->streams as $stream => $cursor)
					$this->xPending($stream);
			}
		} else
			$this->xReadGroup($this->settings->limit, $this->settings->timeout);
	}

	protected function timeToRetry(): int {
		return is_null($this->lastRetry) ? 0 :
			intval($this->settings->retryAfter - $this->lastRetry->diffInMilliseconds(Carbon::now(), false));
	}

	protected function countToRetry(): int {
		if($this->settings->chunkedRetry())
			return $this->leftEntries >= 0 ? $this->leftEntries : $this->settings->retryEvery;
		else
			return -1;
	}

	protected function xReadGroup(int $limit, int $timeout, $acknowledge = false): void {
		$this->blocking = $timeout >= 0;
		$args = ['GROUP', $this->settings->group, $this->settings->consumer];
		$limit > 0 and $args[] = ['COUNT', $limit];
		$timeout >= 0 and $args[] = ['BLOCK', $timeout];
		$acknowledge and $args[] = 'NOACK';
		$args[] = ['STREAMS', array_keys($this->settings->streams), array_values($this->settings->streams)];
		$this->execute(new RedisRequest('XREADGROUP', Arr::flatten($args)));
	}

	protected function xPending(string $stream) {
		$args = [$stream, $this->settings->group, 'IDLE', $this->settings->retryAfter, '-', '+', $this->settings->limit];
		if(!$this->settings->retryForeign)
			$args[] = $this->settings->consumer;
		$this->queue->enqueue($request = new RedisRequest('XPENDING', $args));
		$this->execute($request);
	}

	protected function xClaim(string $stream, array $ids) {
		$args = [$stream, $this->settings->group, $this->settings->consumer, $this->settings->retryAfter, $ids];
		$this->queue->enqueue($request = new RedisRequest('XCLAIM', Arr::flatten($args)));
		$this->execute($request);
	}

	protected function xAcknowledge(string $stream, array $ids) {
		$args = [$stream, $this->settings->group, $ids];
		$this->queue->enqueue($request = new RedisRequest('XACK', Arr::flatten($args)));
		$this->execute($request);
	}

	protected function execute(RedisRequest $request) {
		#debug_log("{$request->command} ".implode(' ', $request->arguments));
		$this->io->write($this->serializer->getRequestMessage($request->command, $request->arguments));
	}

	protected function data(mixed $data): void {
		try { $responses = $this->parser->pushIncoming($data); }
		catch(ParserException $error) {
			$this->emit('error', [new InvalidDataException($error)]);
			$this->close();
			return;
		}

		array_walk($responses, fn(ModelInterface $response) => $this->handle($response));
		// Idle call may be moved into future tick if the call stack still grows.
		if($this->ending)
			$this->close();
		elseif($this->running() && $this->queue->isEmpty())
			$this->idle();
	}

	protected function handle(ModelInterface $response): void {
		if(!($response instanceof ErrorReply)) {
			$request = $this->queue->isEmpty() ? null : $this->queue->dequeue();
			switch($request?->command) {
				case 'XPENDING': $this->handlePending($response, $request); break;
				case 'XCLAIM': $this->handleClaim($response, $request); break;
				case 'XACK': break; // Doing nothing but could check the counts.
				default: $this->handleRead($response); break; // XREAD/XREADGROUP
			}
		} else
			$this->emit('error', [$response]);
	}

	protected function handleRead(ModelInterface $response): void {
		$this->blocking = false;
		if($this->dropping) { // Abort last blocking operation.
			$this->dropping = false;
			return;
		}

		$data = $response->getValueNative();
		$data = !is_null($data) ? $this->structurize($data) : [];
		#empty($data) or debug_log('    '.json_encode($data));
		foreach($this->settings->streams as $name => $cursor) {
			$stream = $data[$name] ?? [];
			if($this->settings->scoped()) {
				if($cursor != '>')
					$this->settings->streams[$name] = $stream ? array_key_last($stream) : '>';
			} elseif($stream)
				$this->settings->streams[$name] = array_key_last($stream);

			if($this->settings->durable() && $this->settings->chunkedRetry() && count($stream) > 0)
				$this->leftEntries = max(0, $this->countToRetry() - count($stream));
		}
		empty($data) or $this->emit('read', [$data, $this->settings->streams]);
	}

	protected function handlePending(ModelInterface $response, RedisRequest $request): void {
		list($stream, $claimable, $failed, $total) = [$request->arguments[0], [], [], 0];
		foreach($response->getValueNative() ?: [] as $temp) {
			list($id, $consumer, $idle, $retries) = $temp;
			#debug_log("    {$id} {$consumer} {$idle} {$retries} {$stream}");
			if($this->settings->failableRetry() && $retries >= $this->settings->maxRetries)
				$failed[] = ['id' => $id, 'retries' => $retries, 'last_at' => Carbon::now()->subMilliseconds($idle),
					'consumer' => $consumer, 'group' => $this->settings->group, 'stream' => $stream];
			else
				$claimable[] = $id;
			$total++;
		}
		if(count($failed) > 0) {
			$this->xAcknowledge($stream, array_column($failed, 'id'));
			$this->emit('fail', [$failed]);
		}
		if(count($claimable) > 0)
			$this->xClaim($stream, $claimable);

		if($total > 0 && $this->settings->limited() && $total >= $this->settings->limit)
			$this->xPending($stream); // Request the next page.
	}

	protected function handleClaim(ModelInterface $response, RedisRequest $request): void {
		$data = $response->getValueNative();
		$data = !is_null($data) ? $this->structurize([[$request->arguments[0], $response->getValueNative()]]) : [];
		$this->emit('read', [$data, $this->settings->streams]); // TODO Retry counts.
	}

	protected function structurize(iterable $data): array {
		$streams = [];
		foreach($data as $a) {
			if(is_null($a[0]) || is_null($a[1]))
				continue;
			$stream = [];
			foreach($a[1] as $b) {
				if(is_null($b[0]) || is_null($b[1]))
					continue;
				$record = [];
				$i = is_array($b[1]) ? new \ArrayIterator($b[1]) : new \IteratorIterator($b[1]);
				while($i->valid()) {
					$k = $i->current(); $i->next();
					$record[$k] = $i->current(); $i->next();
				}
				if(!empty($record))
					$stream[$b[0]] = $record;
			}
			if(!empty($stream))
				$streams[$a[0]] = $stream;
		}
		return $streams;
	}
}

//function debug_log(string $line): void {
//	file_put_contents('debug.log', Carbon::now()->toTimeString('millisecond').': '.$line.PHP_EOL, FILE_APPEND);
//}