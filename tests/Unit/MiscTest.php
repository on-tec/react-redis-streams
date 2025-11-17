<?php

use function \React\Async\await;
use \Ontec\ReactRedisStreams\Client;
use \Ontec\ReactRedisStreams\AuthenticationException;
use \Ontec\ReactRedisStreams\ConnectionCloseException;
use React\EventLoop\Loop as ReactLoop;

list('URL' => $url, 'STREAM' => $stream, 'GROUP' => $group, 'CONSUMER' => $consumer) = $_ENV;

test('Connection refused', function() {
	await((new Client('redis://192.168.0.2:6381', ReactLoop::get()))->ping());
})->throws(\RuntimeException::class, 'Connection refused');

test('Connection timeout', function() {
	await((new Client('redis://192.168.0.224:6379?timeout=1', ReactLoop::get()))->ping());
})->throws(\RuntimeException::class, 'Connection to 192.168.0.224:6379 timed out');

test('Wrong password', function() {
	await((new Client('redis://:throw@192.168.0.2:6380', ReactLoop::get()))->ping());
})->throws(AuthenticationException::class, 'WRONGPASS');

test('No authentication', function() {
	await((new Client('redis://192.168.0.2:6380/0', ReactLoop::get()))->ping());
})->throws(AuthenticationException::class, 'NOAUTH');

test('Wrong database', function() {
	await((new Client('redis://:123456@192.168.0.2:6380/1', ReactLoop::get()))->ping());
})->throws(\OutOfBoundsException::class, 'ERR DB');

test('Answer decay', function() use($stream) {
	await((new Client('redis://:123456@192.168.0.2:6380?decay=1', ReactLoop::get()))
		->xRead('block', 1500, 'streams', $stream, '$'));
})->throws(ConnectionCloseException::class, 'Connection clos');
