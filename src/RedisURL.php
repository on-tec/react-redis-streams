<?php

namespace Ontec\ReactRedisStreams;

final class RedisURL
{
	public readonly string $scheme;
	public readonly string $hostname;
	public readonly int $port;
	public readonly ?string $username;
	public readonly ?string $password;
	public readonly int $database;
	public readonly float $timeout;
	public readonly float $decay;

	public function __construct(protected string $url) {
		if(!preg_match('#^([A-Za-z][0-9A-Za-z+\-.]*)://#', $url))
			$url = (str_starts_with($url, '/') ? 'redis+unix://' : 'redis://').$url;
		if(str_starts_with($url, 'redis+unix:///'))
			$url = substr_replace($url, 'localhost', strlen('redis+unix://') - 1, 0);

		$url = parse_url($url);
		if(!in_array($this->scheme = $url['scheme'] ?? null, ['redis', 'rediss', 'redis+unix']))
			throw new \InvalidArgumentException('Invalid Redis URI given (EINVAL)', defined('SOCKET_EINVAL') ? SOCKET_EINVAL : 22);
		if(!filled($this->hostname = $url[$this->scheme != 'redis+unix' ? 'host' : 'path'] ?? null))
			throw new \InvalidArgumentException('Invalid Redis URI given (EINVAL)', defined('SOCKET_EINVAL') ? SOCKET_EINVAL : 22);
		$this->port = intval($url['port'] ?? 6379);

		$path = trim($url['path'] ?? '', '/');
		$query = []; !empty($url['query']) and parse_str($url['query'], $query);

		$this->username = $this->urlDecode($url['user'] ?? null) ?: ($query['username'] ?? null);
		$this->password = $this->urlDecode($url['pass'] ?? null) ?: ($query['password'] ?? null);
		$this->database = match(true) {
			is_numeric($path) => intval($path),
			isset($query['db']) => intval($query['db']),
			isset($query['database']) => intval($query['database']),
			default => -1
		};
		$this->timeout = floatval($query['timeout'] ?? ini_get('default_socket_timeout'));
		$this->decay = floatval($query['decay'] ?? 0);
	}

	public function getSocketURL(): string {
		return match($this->scheme) {
			'redis' => $this->hostname.':'.$this->port,
			'rediss' => 'tls://'.$this->hostname.':'.$this->port,
			'redis+unix://' => 'unix://'.$this->hostname,
		};
	}

	public function getCredentials(): ?array {
		return !is_null($this->username) || !is_null($this->password)
			? [$this->username, $this->password] : null;
	}

	private function urlDecode(?string $value): ?string {
		$value = rawurldecode($value ?? '');
		return $value !== '' ? $value : null;
	}
}