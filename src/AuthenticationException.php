<?php

namespace Ontec\ReactRedisStreams;

class AuthenticationException extends \Exception {
	public function __construct(string $message = '', ?\Throwable $previous = null) {
		$code = $previous?->getCode() ?: (defined('SOCKET_EACCES') ? SOCKET_EACCES : 13);
		parent::__construct($message ?: $previous?->getMessage() ?: '', $code, $previous);
	}
}