<?php

/*
|--------------------------------------------------------------------------
| Test Case
|--------------------------------------------------------------------------
|
| The closure you provide to your test functions is always bound to a specific PHPUnit test
| case class. By default, that class is "PHPUnit\Framework\TestCase". Of course, you may
| need to change it using the "uses()" function to bind a different classes or traits.
|
*/

// uses(Tests\TestCase::class)->in('Feature');

/*
|--------------------------------------------------------------------------
| Expectations
|--------------------------------------------------------------------------
|
| When you're writing tests, you often need to check that values meet certain conditions. The
| "expect()" function gives you access to a set of "expectations" methods that you can use
| to assert different things. Of course, you may extend the Expectation API at any time.
|
*/

expect()->extend('test', function(string $value) {
	// Assertions based on `$this->value` and the given arguments...

	return $this; // Return this, so another expectations can chain this one...
});

/*
|--------------------------------------------------------------------------
| Functions
|--------------------------------------------------------------------------
|
| While Pest is very powerful out-of-the-box, you may have some testing code specific to your
| project that you don't want to repeat in every file. Here you can also expose helpers as
| global functions to help you to reduce the number of lines of code in your test files.
|
*/

function awaitOneEvent(\Ontec\ReactRedisStreams\Client $redis, string $event): mixed {
	$result = new \React\Promise\Deferred();
	$redis->on($event, fn($data) => $result->resolve($data))
		->on('error', fn($data) => $result->reject($data));
	$data = \React\Async\await($result->promise());
	$redis->removeAllListeners();
	return $data;
}

function debug_log(string $line, bool $anew = false): void {
	file_put_contents('debug.log', $line !== '' ? $line.PHP_EOL : '', $anew ? 0 : FILE_APPEND);
}
