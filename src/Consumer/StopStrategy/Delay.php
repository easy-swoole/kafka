<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 下午9:53
 */
namespace EasySwoole\Kafka\Consumer\StopStrategy;

use EasySwoole\Kafka\Consumer;
use EasySwoole\Kafka\Consumer\StopStrategy;

final class Delay implements StopStrategy
{
    /**
     * The amount of time, in milliseconds, to stop the consumer
     *
     * @var int
     */
    private $delay;

    public function __construct(int $delay)
    {
        $this->delay = $delay;
    }

    public function setup(Consumer $consumer): void
    {
        Loop::delay(
            $this->delay,
            function () use ($consumer): void {
                $consumer->stop();
            }
        );
    }
}
