<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午10:30
 */
namespace EasySwoole\Kafka\Config;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Exception;

class OffsetConfig extends Config
{
    use Singleton;

    protected static $defaults = [
        'groupId'          => '',
        'sessionTimeout'   => 30000,
        'rebalanceTimeout' => 30000,
        'topics'           => [],
        'offsetReset'      => 'latest', // earliest
        'maxBytes'         => 65536, // 64kb
        'maxWaitTime'      => 100,
    ];

    /**
     * @return string
     * @throws Exception\Config
     */
    public function getGroupId(): string
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    /**
     * @param string $groupId
     * @throws Exception\Config
     */
    public function setGroupId(string $groupId): void
    {
        $groupId = trim($groupId);

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Set group id value is invalid, must set it not empty string');
        }

        static::$options['groupId'] = $groupId;
    }
}
