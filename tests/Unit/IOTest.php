<?php

use function \React\Async\await;
use \Carbon\CarbonInterval;
use \Ontec\ReactRedisStreams\Client;
use \Ontec\ReactRedisStreams\Entry;

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
	$redis->timeout(CarbonInterval::second())->stream($stream, '0')->run();
	expect(await($result->promise())[$stream] ?? [])->toHaveLength(3, 'Bulk read');
	$redis->removeAllListeners();
})->depends('Stream write');

test('Stream sequential read', function(Client $redis) use($stream) {
	$redis->timeout(CarbonInterval::milliseconds(100))->limit(1)->stream($stream, '0');
	for($i = 0; $i < 3; $i++)
		expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveLength(1);
})->depends('Stream write');

test('Stream consumer read new', function(Client $redis) use($stream, $consumer, $group) {
	await($redis->xGroup('DESTROY', $stream, $group));
	await($redis->xGroup('CREATE', $stream, $group, '0'));
	$redis->reset()->timeout(CarbonInterval::milliseconds(100))->limit(1)
		->scope($consumer, $group)->stream($stream, '0')->run();
	for($i = 0; $i < 3; $i++)
		expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveLength(1);
})->depends('Stream write');

test('Stream consumer read pending', function(Client $redis) use($stream, $consumer, $group) {
	$redis->reset()->timeout(CarbonInterval::milliseconds(100))->limit(1)
		->scope($consumer, $group)->stream($stream, '0')->run();
	for($i = 0; $i < 3; $i++)
		expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveLength(1);
})->depends('Stream write');

test('Stream consumer re-read by expiration', function(Client $redis) use($stream, $consumer, $group) {
	$redis->reset();
	$redis->timeout(CarbonInterval::milliseconds(100))->limit(1)->scope($consumer, $group)
		->retry(CarbonInterval::milliseconds(100), 3)->stream($stream, '>');
	\React\Async\delay(0.1);
	$redis->run();

	$ids = array_fill_keys(['a', 'b', 'c'], null);
	foreach(array_keys($ids) as $key) { // 3rd try.
		$entry = head(awaitOneEvent($redis, 'read')[$stream] ?? []);
		expect($entry)->toHaveKey($key);
		if($key == 'b') {
			$redis->xack($stream, $group, $entry->id);
			$ids['d'] = await($redis->xadd($stream, 'maxlen', '=', 4, '*', 'd', 4));
		} else
			$ids[$key] = $entry->id;
	}

	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($ids['d']);

	unset($ids['b']);
	foreach($ids as $id) { // Fail: a, c.
		$entry = head(awaitOneEvent($redis, 'fail')[$stream] ?? []);
		expect($entry?->id)->toBe($id);
		expect($entry?->retries)->toBe(3);
	}
})->depends('Stream write');

test('Stream consumer re-read by alternation', function(Client $redis) use($stream, $consumer, $group) {
	$redis->reset();
	$redis->timeout(CarbonInterval::milliseconds(100))->limit(1)->scope($consumer, $group)
		->retry(CarbonInterval::day(), 1, 2)->stream($stream, '>');
	\React\Async\delay(0.1);

	$idle = CarbonInterval::week()->totalMilliseconds;
	foreach(['e', 'f', 'g', 'h'] as $i => $v)
		$ids[$v] = await($redis->xadd($stream, 'maxlen', '=', 4, '*', $v, $i + 5));

	$redis->run();
	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($ids['e']);
	$redis->xclaim($stream, $group, $consumer, 0, $ids['e'], 'IDLE', $idle, 'FORCE');
	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($ids['f']);
	expect(awaitOneEvent($redis, 'fail')[$stream] ?? [])->toHaveKey($ids['e']);
	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($ids['g']);
	$redis->xclaim($stream, $group, $consumer, 0, $ids['f'], 'IDLE', $idle, 'FORCE');
	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($ids['h']);
	expect(awaitOneEvent($redis, 'fail')[$stream] ?? [])->toHaveKey($ids['f']);

})->depends('Stream write');

test('High order write & acknowledge', function(Client $redis) use($stream, $consumer, $group) {
	$redis->reset();
	$redis->xGroup('DESTROY', $stream, $group);
	$redis->xGroup('CREATE', $stream, $group, '$');
	$redis->timeout(CarbonInterval::milliseconds(100))->limit(1)->scope($consumer, $group)
		->trim(CarbonInterval::milliseconds(300))->stream($stream, '>');
	$id = await($redis->record(new Entry(['x' => rand()], $stream)));
	\React\Async\delay(0.1);

	$redis->run();
	expect(awaitOneEvent($redis, 'read')[$stream] ?? [])->toHaveKey($id);
	$redis->acknowledge(new Entry($id, $stream));
	expect(await($redis->xpending($stream, $group))[0])->toBe(0);
})->depends('Stream write');


//beforeAll(fn() => debug_log('', true));
//beforeEach(fn() => debug_log('=== '.$this->getPrintableTestCaseMethodName().' ==='));
//afterAll(function() {
//	$loop = React\EventLoop\Loop::get();
//	$loop->addTimer(1, fn() => $loop->stop());
////	$loop->run();
//});