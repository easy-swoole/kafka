<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/22
 * Time: 下午6:51
 */
namespace EasySwoole\Kafka\Exception;

final class ConnectionException extends Exception
{
    public static function fromBrokerList(string $brokerList): self
    {
        return new self(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }
}
