<?php

namespace Ontec\ReactRedisStreams;

use Illuminate\Support\Arr;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register(): void {
		$this->app->bind(Client::class, function($app, $parameters) {
			if(filled($url = $parameters['url'] ?? null))
				$dsn = new RedisURL($url);
			else {
				$class = new \ReflectionClass(ConnectionDSN::class);
				$whitelist = array_map(fn(\ReflectionParameter $p) => $p->getName(),
					$class->getConstructor()?->getParameters() ?? []);
				$dsn = new ConnectionDSN(...Arr::only($parameters, $whitelist));
			}

			if(empty($loop = $parameters['loop'] ?? null))
				$loop = \React\EventLoop\Loop::get();
			return new Client($dsn, $loop);
		});
    }
}