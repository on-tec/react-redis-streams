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
	protected ?Carbon $lastRetry = null;
	protected int $leftEntries = -1;

	public function __construct(protected DuplexStreamInterface $io,
								protected ParserInterface       $parser,
								protected SerializerInterface   $serializer,
								protected StreamSettings        $settings) {
		$this->queue = new \SplQueue();
		$this->settings->durable() and $this->lastRetry = Carbon::now('UTC'); // Use cursor = 0 for first pending iteration.
		$io->on('data', fn(mixed $data) => $this->data($data));
		$io->on('drain', fn() => $this->drain());
		$io->on('close', fn() => $this->close());
		$this->idle();
	}

	public function alive(): bool {
		return !$this->ending && !$this->closed &&
			$this->io->isReadable() && $this->io->isWritable();
	}

	public function end(): static {
		$this->ending = true;
		return $this;
	}

	public function close(): void {
		if(!$this->closed) {
			$this->ending = $this->closed = true;
			$this->io->close();
			$this->emit('close');
		}
	}

	protected function idle(): void {
		if(!$this->ending)
			$this->settings->scoped() ? $this->doReadGroup() : $this->xRead();
		else
			$this->close();
	}

	protected function xRead(): void {
		$this->settings->hasStreams() or throw new \RangeException('No streams selected.');
		$args = [];
		$this->settings->limit and $args[] = ['COUNT', $this->settings->limit];
		$args[] = ['BLOCK', $this->settings->timeout];
		$args[] = ['STREAMS', array_keys($this->settings->streams), array_values($this->settings->streams)];
		$this->execute(new RedisRequest('XREAD', Arr::flatten($args)));
	}

	protected function doReadGroup(): void {
		$this->settings->hasStreams() or throw new \RangeException('No streams selected.');

		if($this->settings->durable()) {
			list($timeToRetry, $countToRetry) = [$this->timeToRetry(), $this->countToRetry()];
			#debug_log("Time left: {$timeToRetry}; count left: {$countToRetry}");
			if($timeToRetry > 0 && $countToRetry != 0) {
				$limit = $countToRetry > 0 ? min($this->settings->limit, $countToRetry) : $this->settings->limit;
				$timeout = $this->settings->endlessBlocking() ? $timeToRetry + 1 : $this->settings->timeout;
				$this->xReadGroup($limit, min($timeToRetry + 1, $timeout));
			} else {
				$this->settings->limited() or throw new \UnexpectedValueException('Entries limit is required for retrospection.');
				$this->lastRetry = Carbon::now('UTC');
				$this->leftEntries = $this->settings->chunkedRetry() ? $this->settings->retryEvery : -1;
				foreach($this->settings->streams as $stream => $cursor)
					$this->xPending($stream);
			}
		} else
			$this->xReadGroup($this->settings->limit, $this->settings->timeout);
	}

	protected function timeToRetry(): int {
		return is_null($this->lastRetry) ? 0 :
			intval($this->settings->retryAfter - $this->lastRetry->diffInMilliseconds(Carbon::now('UTC'), false));
	}

	protected function countToRetry(): int {
		if($this->settings->chunkedRetry())
			return $this->leftEntries >= 0 ? $this->leftEntries : $this->settings->retryEvery;
		else
			return -1;
	}

	protected function xReadGroup(int $limit, int $timeout, $acknowledge = false): void {
		$args = ['GROUP', $this->settings->group, $this->settings->consumer];
		$limit > 0 and $args[] = ['COUNT', $limit];
		$args[] = ['BLOCK', $timeout];
		$acknowledge and $args[] = 'NOACK';
		$args[] = ['STREAMS', array_keys($this->settings->streams), array_values($this->settings->streams)];
		$this->execute(new RedisRequest('XREADGROUP', Arr::flatten($args)));
	}

	protected function xPending(string $stream) {
		$args = [$stream, $this->settings->group, 'IDLE', $this->settings->retryAfter, '-', '+', $this->settings->limit];
		if(!$this->settings->retryForeign)
			$args[] = $this->settings->consumer;
		$this->execute(new RedisRequest('XPENDING', $args), true);
	}

	protected function xClaim(string $stream, array $entries) {
		$args = [$stream, $this->settings->group, $this->settings->consumer, $this->settings->retryAfter, array_keys($entries)];
		$this->execute(new RedisRequest('XCLAIM', Arr::flatten($args), $entries), true);
	}

	protected function xAcknowledge(string $stream, array $ids) {
		$args = [$stream, $this->settings->group, $ids];
		$this->execute(new RedisRequest('XACK', Arr::flatten($args)), true);
	}

	protected function execute(RedisRequest $request, $queue = false) {
		#debug_log("{$request->command} ".implode(' ', $request->arguments));
		$this->io->write($this->serializer->getRequestMessage($request->command, $request->arguments)); // FIXME handle false result & drain event.
		$queue and $this->queue->enqueue($request); // FIXME Don't enqueue if result is false.
	}

	protected function data(mixed $data): void {
		try {
			$responses = $this->parser->pushIncoming($data);
			foreach($responses as $data)
				try { $this->handle($data); }
				catch(\Throwable $e) { $this->emit('error', [$e]); } // Operation or event error.
			if(!$this->ending && $this->queue->isEmpty()) // FIXME Replace ending to alive?
				$this->idle(); // Idle call could be moved into future tick if call stack grows.
		} catch(\Throwable $e) {
			$this->emit('error', [$e instanceof ParserException ? new InvalidDataException($e) : $e]);
			$this->close();
		}

		$this->ending and $this->close();
	}

	protected function drain(): void {
		// FIXME check & idle()
	}

	protected function handle(ModelInterface $response): void {
		$request = $this->queue->isEmpty() ? null : $this->queue->dequeue();
		if(!($response instanceof ErrorReply)) {
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
		list($stream, $entries) = [$request->arguments[0], []];
		foreach($response->getValueNative() ?: [] as $temp) {
			list($id, $consumer, $idle, $retries) = $temp;
			#debug_log("    {$id} {$consumer} {$idle} {$retries} {$stream}");
			$entries[$id] = $entry = new Entry($id, [], $stream);
			$entry->retries = intval($retries);
			$entry->consumer = $consumer;
			$entry->claimed_at = Carbon::now('UTC')->subMilliseconds($idle);
		}

		if(!empty($entries))
			$this->xClaim($stream, $entries);

		if(!empty($entries) && $this->settings->limited() && count($entries) >= $this->settings->limit)
			$this->xPending($stream); // Request the next page.
	}

	protected function handleClaim(ModelInterface $response, RedisRequest $request): void {
		if(!is_null($data = $response->getValueNative())) {
			list($stream, $read, $failed) = [$request->arguments[0], [], []];
			$data = $this->structurize([[$stream, $data]], [$stream => $request->payload]);
			/** @var Entry $entry */
			foreach($data[$stream] as $id => $entry)
				if($this->settings->failableRetry() && $entry->retries >= $this->settings->maxRetries)
					$failed[$id] = $entry;
				else
					$read[$id] = $entry;

			if(!empty($read))
				$this->emit('read', [[$stream => $read], $this->settings->streams]);

			if(!empty($failed)) {
				$this->xAcknowledge($stream, array_keys($failed));
				$this->emit('fail', [[$stream => $failed]]);
			}
		}
	}

	protected function structurize(iterable $data, array $streams = []): array {
		foreach($data as $a) {
			if(is_null($name = $a[0] ?? null) || is_null($a[1] ?? null))
				continue;
			$stream = $streams[$name] ?? [];
			foreach($a[1] as $b) {
				if(is_null($id = $b[0] ?? null) || is_null($b[1] ?? null))
					continue;
				$record = $stream[$id] ?? new Entry($id, $name);
				$i = is_array($b[1]) ? new \ArrayIterator($b[1]) : new \IteratorIterator($b[1]);
				while($i->valid()) {
					$k = $i->current(); $i->next();
					$record[$k] = $i->current(); $i->next();
				}
				if(!$record->isEmpty())
					$stream[$id] = $record;
				else
					unset($stream[$id]);
			}
			if(!empty($stream))
				$streams[$name] = $stream;
		}
		return $streams;
	}
}

//function debug_log(string $line): void {
//	file_put_contents('debug.log', Carbon::now()->toTimeString('millisecond').': '.$line.PHP_EOL, FILE_APPEND);
//}
