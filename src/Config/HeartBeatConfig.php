<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:35
 */
namespace EasySwoole\Kafka\Config;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Exception;

/**
 * Class HeartBeatConfig
 * @method string getGroupId()
 * @package EasySwoole\Kafka\Config
 */
class HeartBeatConfig extends Config
{
    use Singleton;

    protected static $defaults = [
        'groupId'          => '',
    ];

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
