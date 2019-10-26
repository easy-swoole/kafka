<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:17
 */
namespace EasySwoole\Kafka;

interface SaslMechanism
{
    public function autheticate(Client $client): void;
}
