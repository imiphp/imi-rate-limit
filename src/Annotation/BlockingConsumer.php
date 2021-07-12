<?php

namespace Imi\RateLimit\Annotation;

use Imi\Bean\Annotation\Base;
use Imi\Bean\Annotation\Parser;

/**
 * 阻塞等待消费.
 *
 * 当触发限流时，自动阻塞（协程挂起）等待
 *
 * @Annotation
 * @Target("METHOD")
 * @Parser("\Imi\Bean\Parser\NullParser")
 */
class BlockingConsumer extends Base
{
    /**
     * 只传一个参数时的参数名.
     *
     * @var string
     */
    protected $defaultFieldName = 'timeout';

    /**
     * 超时时间，单位：秒.
     *
     * 为 null 不限制
     *
     * @var int|null
     */
    public $timeout;
}
