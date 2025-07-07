<?php

namespace Ontec\ReactRedisStreams;

use Clue\Redis\Protocol\Model\ErrorReply;
use Clue\Redis\Protocol\Model\ModelInterface;
use Clue\Redis\Protocol\Parser\ParserException;
use Clue\Redis\Protocol\Parser\ParserInterface;
use Clue\Redis\Protocol\Serializer\SerializerInterface;
use Evenement\EventEmitter;
use React\Promise\PromiseInterface;
use React\Stream\DuplexStreamInterface;

/**
 * @method PromiseInterface select(int $index)
 */
class ControlConnection extends EventEmitter implements ConnectionInterface
{
	protected \SplQueue $queue;
	protected bool $ending = false;
	protected bool $closed = false;

	public function __construct(protected DuplexStreamInterface $io,
								protected ParserInterface       $parser,
								protected SerializerInterface   $serializer) {
		$this->queue = new \SplQueue();
		$io->on('data', fn(mixed $data) => $this->data($data));
		$io->on('close', fn() => $this->close());
	}

	public function auth(string $password, ?string $username = null): PromiseInterface {
		return is_null($username) ? $this->__call('auth', [$password]) : $this->__call('auth', [$password, $username]);
	}

	public function alive(): bool {
		return !$this->ending && !$this->closed &&
			$this->io->isReadable() && $this->io->isWritable();
	}

	public function __call(string $command, array $arguments): PromiseInterface {
        $request = new RedisRequest($command = strtoupper($command), $arguments);

        if($this->ending) {
			$request->reject(new ConnectionCloseException(!$this->closed, 'SOCKET_ENOTCONN'));
        } elseif($command === 'MONITOR') {
            $request->reject(new \BadMethodCallException('MONITOR command explicitly not supported (ENOTSUP)',
                defined('SOCKET_EOPNOTSUPP') ? SOCKET_EOPNOTSUPP : 95));
        } else {
            $this->io->write($this->serializer->getRequestMessage($command, $arguments));
			$this->queue->enqueue($request);
        }

        return $request->promise();
	}

	public function end(): static
	{
		$this->ending = true;
		$this->queue->isEmpty() and $this->close();
		return $this;
	}

	public function close(): void
	{
		if($this->closed)
			return;

		$this->ending = $this->closed = true;
		$remotely = !$this->io->isReadable() && !$this->io->isWritable();
		$this->io->close();

		$this->emit('close');

		// Rejecting all remaining requests in the queue.
		$this->queue->setIteratorMode(\SplQueue::IT_MODE_DELETE);
		foreach($this->queue as $request) {
			assert($request instanceof RedisRequest);
			$request->reject($remotely ? new ConnectionCloseException(false, 'SOCKET_ECONNRESET')
				: new ConnectionCloseException(true, 'SOCKET_ECONNABORTED'));
		}
	}

	protected function data(mixed $data): void {
		try { $responses = $this->parser->pushIncoming($data); }
		catch(ParserException $error) {
			$this->emit('error', [new InvalidDataException($error)]);
			$this->close();
			return;
		}

		foreach($responses as $data) {
			try { $this->handle($data); }
			catch(\UnderflowException $error) {
				$this->emit('error', [$error]);
				$this->close();
				return;
			}
		}
	}

	protected function handle(ModelInterface $message): void {
		if(!$this->queue->isEmpty()) {
			$request = $this->queue->dequeue();
			assert($request instanceof RedisRequest);
			if(!($message instanceof ErrorReply))
				$request->resolve($message->getValueNative());
			elseif(str_starts_with(ltrim($message->getMessage()), 'NOAUTH'))
				$request->reject(new AuthenticationException($message->getMessage()));
			else
				$request->reject($message);
		} else
			throw new \UnderflowException('Unexpected reply received, no matching request found (ENOMSG)',
				defined('SOCKET_ENOMSG') ? SOCKET_ENOMSG : 42);

		if($this->ending && $this->queue->isEmpty())
			$this->close();
	}
}