<?php

namespace Ontec\ReactRedisStreams;

class ConnectionDSN
{
	public readonly string $hostname;
	public readonly int $port;
	public readonly ?string $username; // FIXME Make non-nullable.
	public readonly ?string $password; // FIXME Make non-nullable.
	public readonly int $database;
	public readonly float $timeout;
	public readonly float $decay;

	public function __construct(
		string $hostname = 'localhost',
		int $port = 6379,
		string $username = '',
		string $password = '',
		int $database = -1,
		float $timeout = -1,
		float $decay = 0,
	) {
		$this->hostname = $hostname;
		$this->port = $port;
		$this->username = filled($username) ? $username : null;
		$this->password = filled($password) ? $password : null;
		$this->database = $database;
		$this->timeout = $timeout >= 0 ? $timeout : ini_get('default_socket_timeout');
		$this->decay = $decay;
	}

	public function getSocketURL(): string {
		return $this->hostname.':'.$this->port;
	}

	public function getCredentials(): ?array {
		return filled($this->username) || filled($this->password)
			? [$this->username, $this->password] : null;
	}
}