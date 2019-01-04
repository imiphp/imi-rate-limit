<?php
namespace Imi\RateLimit\Aspect;

use Imi\Aop\PointCutType;
use Imi\Bean\BeanFactory;
use Imi\Aop\AroundJoinPoint;
use Imi\Aop\Annotation\Around;
use Imi\Aop\Annotation\Aspect;
use Imi\RateLimit\RateLimiter;
use Imi\Aop\Annotation\PointCut;
use Imi\RateLimit\Annotation\RateLimit;
use Imi\Bean\Annotation\AnnotationManager;
use Imi\RateLimit\Annotation\BlockingConsumer;

/**
 * @Aspect
 */
class RateLimitAspect
{
    /**
     * 处理限流
     * @PointCut(
     *         type=PointCutType::ANNOTATION,
     *         allow={
     *             RateLimit::class
     *         }
     * )
     * @Around
     * @return mixed
     */
    public function parse(AroundJoinPoint $joinPoint)
    {
        $className = BeanFactory::getObjectClass($joinPoint->getTarget());
        $method = $joinPoint->getMethod();
        $rateLimit = AnnotationManager::getMethodAnnotations($className, $method, RateLimit::class)[0] ?? null;
        $blockingConsumer = AnnotationManager::getMethodAnnotations($className, $method, BlockingConsumer::class)[0] ?? null;
        if(null === $blockingConsumer)
        {
            $result = RateLimiter::limit($rateLimit->name, $rateLimit->capacity, $rateLimit->callback, $rateLimit->fill, $rateLimit->unit, $rateLimit->deduct, $rateLimit->poolName);
        }
        else
        {
            $result = RateLimiter::limitBlock($rateLimit->name, $rateLimit->capacity, $rateLimit->callback, $blockingConsumer->timeout, $rateLimit->fill, $rateLimit->unit, $rateLimit->deduct, $rateLimit->poolName);
        }
        if(true === $result)
        {
            return $joinPoint->proceed();
        }
        else
        {
            return $result;
        }
    }
}
