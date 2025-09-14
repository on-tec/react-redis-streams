<?php

use function \React\Async\await;
use \Ontec\ReactRedisStreams\Client;
use \Ontec\ReactRedisStreams\AuthenticationException;
use \Ontec\ReactRedisStreams\ConnectionCloseException;

list('URL' => $url, 'STREAM' => $stream, 'GROUP' => $group, 'CONSUMER' => $consumer) = $_ENV;

test('Connection refused', function() {
	await((new Client('redis://192.168.0.2:6381', React\EventLoop\Loop::get()))->ping());
})->throws(\RuntimeException::class, 'Connection refused');

test('Connection timeout', function() {
	await((new Client('redis://192.168.0.224:6379?timeout=1', React\EventLoop\Loop::get()))->ping());
})->throws(\RuntimeException::class, 'Connection to 192.168.0.224:6379 timed out');

test('Wrong password', function() {
	await((new Client('redis://:throw@192.168.0.2:6380', React\EventLoop\Loop::get()))->ping());
})->throws(AuthenticationException::class, 'WRONGPASS');

test('No authentication', function() {
	await((new Client('redis://192.168.0.2:6380/0', React\EventLoop\Loop::get()))->ping());
})->throws(AuthenticationException::class, 'NOAUTH');

test('Wrong database', function() {
	await((new Client('redis://:123456@192.168.0.2:6380/1', React\EventLoop\Loop::get()))->ping());
})->throws(\OutOfBoundsException::class, 'ERR DB');

test('Answer decay', function() use($stream) {
	await((new Client('redis://:123456@192.168.0.2:6380?decay=1', React\EventLoop\Loop::get()))
		->xRead('block', 1500, 'streams', $stream, '$'));
})->throws(ConnectionCloseException::class, 'Connection clos');

afterAll(function() {
	$loop = React\EventLoop\Loop::get();
	$loop->addTimer(1, fn() => $loop->stop());
//	$loop->run();
});