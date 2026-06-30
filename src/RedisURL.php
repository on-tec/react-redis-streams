<?php

namespace Ontec\ReactRedisStreams;

final class RedisURL extends ConnectionDSN
{
	public readonly string $scheme;

	public function __construct(protected string $url) {
		if(!preg_match('#^([A-Za-z][0-9A-Za-z+\-.]*)://#', $url))
			$url = (str_starts_with($url, '/') ? 'redis+unix://' : 'redis://').$url;
		if(str_starts_with($url, 'redis+unix:///'))
			$url = substr_replace($url, 'localhost', strlen('redis+unix://') - 1, 0);

		$url = parse_url($url);
		if(!in_array($this->scheme = $url['scheme'] ?? null, ['redis', 'rediss', 'redis+unix']))
			throw new \InvalidArgumentException('Invalid Redis URI given (EINVAL)', defined('SOCKET_EINVAL') ? SOCKET_EINVAL : 22);
		if(!filled($hostname = $url[$this->scheme != 'redis+unix' ? 'host' : 'path'] ?? null))
			throw new \InvalidArgumentException('Invalid Redis URI given (EINVAL)', defined('SOCKET_EINVAL') ? SOCKET_EINVAL : 22);

		$path = trim($url['path'] ?? '', '/');
		$query = []; !empty($url['query']) and parse_str($url['query'], $query);

		parent::__construct(
			hostname: $hostname,
			port: intval($url['port'] ?? 6379),
			username: $this->urlDecode($url['user'] ?? null) ?: ($query['username'] ?? null) ?: '',
			password: $this->urlDecode($url['pass'] ?? null) ?: ($query['password'] ?? null) ?: '',
			database: match(true) {
				is_numeric($path) => intval($path),
				isset($query['db']) => intval($query['db']),
				isset($query['database']) => intval($query['database']),
				default => -1
			},
			timeout: floatval($query['timeout'] ?? -1),
			decay: floatval($query['decay'] ?? 0));
	}

	public function getSocketURL(): string {
		return match($this->scheme) {
			'redis' => $this->hostname.':'.$this->port,
			'rediss' => 'tls://'.$this->hostname.':'.$this->port,
			'redis+unix://' => 'unix://'.$this->hostname,
		};
	}

	private function urlDecode(?string $value): ?string {
		$value = rawurldecode($value ?? '');
		return $value !== '' ? $value : null;
	}
}