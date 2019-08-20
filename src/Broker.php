<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午10:59
 */
namespace EasySwoole\Kafka;

use EasySwoole\Component\Singleton;

class Broker
{
    use Singleton;

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @return array
     */
    public function getTopics (): array
    {
        return $this->topics;
    }

    /**
     * @return array
     */
    public function getBrokers (): array
    {
        return $this->brokers;
    }


}