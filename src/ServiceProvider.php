<?php

namespace Ontec\ReactRedisStreams;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register(): void {
		$this->app->bind(Client::class, function($app, $parameters) {
			if(empty($url = $parameters['url'] ?? null))
				throw new \InvalidArgumentException('Redis connection URL is required.');
			if(empty($loop = $parameters['loop'] ?? null))
				$loop = \React\EventLoop\Loop::get();
			return new Client($url, $loop);
		});
    }
}