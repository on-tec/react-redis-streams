<?php

namespace Ontec\ReactRedisStreams;

class ConnectionDSN
{
	public readonly string $hostname;
	public readonly int $port;
	public readonly string $username;
	public readonly string $password;
	public readonly int $database;
	public readonly float $timeout;
	public readonly float $decay;

	public function __construct(?string $hostname = null, ?int $port = null,
								?string $username = null, ?string $password = null,
								?int $database = null, ?float $timeout = null, ?float $decay = null) {
		$this->hostname = $hostname ?: 'localhost';
		$this->port = $port ?: 6379;
		$this->username = $username ?: '';
		$this->password = $password ?: '';
		$this->database = $database ?: -1;
		$this->timeout = $timeout >= 0 ? $timeout : ini_get('default_socket_timeout');
		$this->decay = $decay ?: 0;
	}

	public function getSocketURL(): string {
		return $this->hostname.':'.$this->port;
	}

	public function getCredentials(): ?array {
		return filled($this->username) || filled($this->password)
			? [$this->username, $this->password] : null;
	}
}