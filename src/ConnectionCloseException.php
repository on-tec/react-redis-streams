<?php

namespace Ontec\ReactRedisStreams;

use Throwable;

class ConnectionCloseException extends \RuntimeException
{
	public function __construct(bool $waiting = false, string $status = '', ?Throwable $previous = null) {
		$message = 'Connection '.($waiting ? 'closed' : 'closing').($status ? " ({$status})." : '.');
		$code = defined($status) ? constant($status) : 0;
		parent::__construct($message, $code, $previous);
	}
}