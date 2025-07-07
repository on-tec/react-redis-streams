<?php

namespace Ontec\ReactRedisStreams;

use Clue\Redis\Protocol\Parser\ParserException;

class InvalidDataException extends \UnexpectedValueException
{
	public function __construct(ParserException $error) {
		$message = $error->getMessage().' (SOCKET_EBADMSG).';
		parent::__construct($message, defined('SOCKET_EBADMSG') ? SOCKET_EBADMSG : 77, $error);
	}
}