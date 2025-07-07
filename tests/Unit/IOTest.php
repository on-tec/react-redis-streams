<?php

use function \React\Async\await;
use \Ontec\ReactRedisStreams\Client;

list('URL' => $url, 'STREAM' => $stream, 'GROUP' => $group, 'CONSUMER' => $consumer) = $_ENV;

test('Redis connect', function() use($url) {
	$redis = new Client($url, React\EventLoop\Loop::get());
	expect(await($redis->ping()))->toBe('PONG');
	return $redis;
});

test('Stream write', function(Client $redis) use($stream) {
	await($redis->xadd($stream, 'maxlen', '=', 3, '*', 'a', 1));
	await($redis->xadd($stream, 'maxlen', '=', 3, '*', 'b', 2));
	await($redis->xadd($stream, 'maxlen', '=', 3, '*', 'c', 3));
	expect(await($redis->xlen($stream)))->toBe(3);
	return $redis;
})->depends('Redis connect');

test('Stream bulk read', function(Client $redis) use($stream) {
	$result = new \React\Promise\Deferred();
	$redis->on('read', fn($data) => $result->resolve($data))->on('error', fn($data) => $result->reject($data));
	$redis->timeout(1)->stream($stream, '0')->run();
	expect(await($result->promise())[$stream] ?? [])->toHaveLength(3, 'Bulk read');
	$redis->removeAllListeners();
})->depends('Stream write');

test('Stream sequential read', function(Client $redis) use($stream) {
	$redis->timeout(0.1)->limit(1)->stream($stream, '0');
	for($i = 0; $i < 5; $i++) {
		$result = new \React\Promise\Deferred();
		$redis->on('read', fn($data) => $result->resolve($data))->on('error', fn($data) => $result->reject($data));
		expect(await($result->promise())[$stream] ?? [])->toHaveLength($i < 3 ? 1 : 0);
		$redis->removeAllListeners();
	}
})->depends('Stream write');

test('Stream consumer read new', function(Client $redis) use($stream, $consumer, $group) {
	await($redis->xGroup('DESTROY', $stream, $group));
	await($redis->xGroup('CREATE', $stream, $group, '0'));
	$redis->timeout(0.1)->limit(1)->consumer($consumer, $group)->stream($stream, '0');

	for($i = 0; $i < 5; $i++) {
		$result = new \React\Promise\Deferred();
		$redis->on('read', fn($data) => $result->resolve($data))->on('error', fn($data) => $result->reject($data));
		$data = await($result->promise())[$stream] ?? [];
		expect($data)->toHaveLength($i > 0 && $i < 4 ? 1 : 0);
		$redis->removeAllListeners();
	}
})->depends('Stream write');

test('Stream consumer read pending', function(Client $redis) use($stream, $consumer, $group) {
	$redis->timeout(0.1)->limit(1)->consumer($consumer, $group)->stream($stream, '0');

	for($i = 0; $i < 5; $i++) {
		$result = new \React\Promise\Deferred();
		$redis->on('read', fn($data) => $result->resolve($data))->on('error', fn($data) => $result->reject($data));
		$data = await($result->promise())[$stream] ?? [];
		expect($data)->toHaveLength($i < 3 ? 1 : 0);
		$redis->removeAllListeners();
	}
})->depends('Stream write');
