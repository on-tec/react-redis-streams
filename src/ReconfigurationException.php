<?php

namespace Ontec\ReactRedisStreams;

class ReconfigurationException extends \RuntimeException
{
	public static function running() {
		return new ReconfigurationException('Configuration can not be done on running stream.');
	}
}