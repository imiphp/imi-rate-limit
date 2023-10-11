<?php

declare(strict_types=1);

namespace Imi\RateLimit;

use bandwidthThrottle\tokenBucket\BlockingConsumer;
use bandwidthThrottle\tokenBucket\Rate;
use bandwidthThrottle\tokenBucket\TimeoutException;
use bandwidthThrottle\tokenBucket\TokenBucket;
use Imi\RateLimit\Exception\RateLimitException;
use Imi\RateLimit\Storage\ImiRedisStorage;
use Imi\Redis\RedisManager;

/**
 * 限流器手动调用类.
 */
class RateLimiter
{
    use \Imi\Util\Traits\TStaticClass;

    /**
     * 限流
     *
     * @param string        $name     限流器名称
     * @param int           $capacity 总容量
     * @param callable|null $callback 触发限流的回调
     * @param int|null      $fill     单位时间内生成填充的数量，不设置或为null时，默认值与 $capacity 相同
     * @param string        $unit     单位时间，默认为：秒(second)，支持：microsecond、millisecond、second、minute、hour、day、week、month、year
     * @param int           $deduct   每次扣除数量，默认为1
     * @param string|null   $poolName 连接池名称，留空取默认 redis 连接池
     *
     * @return mixed
     */
    public static function limit(string $name, int $capacity, ?callable $callback = null, ?int $fill = null, string $unit = 'second', int $deduct = 1, ?string $poolName = null)
    {
        if (null === $fill)
        {
            $fill = $capacity;
        }
        $storage = new ImiRedisStorage($name, RedisManager::getInstance($poolName));
        $rate = new Rate($fill, $unit);
        $bucket = new TokenBucket($capacity, $rate, $storage);
        $bucket->bootstrap($capacity);
        if (!$bucket->consume($deduct))
        {
            if ($callback)
            {
                return $callback($name);
            }
            else
            {
                return static::defaultCallback($name);
            }
        }
        else
        {
            return true;
        }
    }

    /**
     * 限流，允许超时等待.
     *
     * @param string        $name            限流器名称
     * @param int           $capacity        总容量
     * @param callable|null $callback        触发限流的回调
     * @param int|null      $blockingTimeout 超时时间，单位：秒;为 null 不限制
     * @param int|null      $fill            单位时间内生成填充的数量，不设置或为null时，默认值与 $capacity 相同
     * @param string        $unit            单位时间，默认为：秒(second)，支持：microsecond、millisecond、second、minute、hour、day、week、month、year
     * @param int           $deduct          每次扣除数量，默认为1
     * @param string|null   $poolName        连接池名称，留空取默认 redis 连接池
     *
     * @return mixed
     */
    public static function limitBlock(string $name, int $capacity, ?callable $callback = null, ?int $blockingTimeout = null, ?int $fill = null, string $unit = 'second', int $deduct = 1, ?string $poolName = null)
    {
        if (null === $fill)
        {
            $fill = $capacity;
        }
        $storage = new ImiRedisStorage($name, RedisManager::getInstance($poolName), $blockingTimeout);
        $rate = new Rate($fill, $unit);
        $bucket = new TokenBucket($capacity, $rate, $storage);
        $bucket->bootstrap($capacity);
        $consumer = new BlockingConsumer($bucket, $blockingTimeout);
        try
        {
            $consumer->consume($deduct);

            return true;
        }
        catch (TimeoutException $ex)
        {
            if ($callback)
            {
                return $callback($name);
            }
            else
            {
                return static::defaultCallback($name);
            }
        }
    }

    /**
     * 获取可用数量.
     *
     * @param string      $name     限流器名称
     * @param int         $capacity 总容量
     * @param int|null    $fill     单位时间内生成填充的数量，不设置或为null时，默认值与 $capacity 相同
     * @param string      $unit     单位时间，默认为：秒(second)，支持：microsecond、millisecond、second、minute、hour、day、week、month、year
     * @param string|null $poolName 连接池名称，留空取默认 redis 连接池
     */
    public static function getTokens(string $name, int $capacity, ?int $fill = null, string $unit = 'second', ?string $poolName = null): int
    {
        $storage = new ImiRedisStorage($name, RedisManager::getInstance($poolName));
        $rate = new Rate($fill ?? $capacity, $unit);
        $bucket = new TokenBucket($capacity, $rate, $storage);
        $bucket->bootstrap($capacity);

        return $bucket->getTokens();
    }

    /**
     * 默认限流回调.
     *
     * @param string $name 限流器名称
     *
     * @return mixed
     */
    public static function defaultCallback(string $name)
    {
        throw new RateLimitException(sprintf('%s Rate Limit', $name));
    }
}
