<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 下午9:49
 */
namespace EasySwoole\Kafka\Consumer;

use EasySwoole\Kafka\Consumer;

interface StopStrategy
{
    public function setup(Consumer $consumer): void;
}
