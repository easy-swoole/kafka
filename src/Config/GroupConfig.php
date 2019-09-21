<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午4:03
 */
namespace EasySwoole\Kafka\Config;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Exception;

/**
 * Class GroupConfig
 * @method string getGroupId()
 * @method int getSessionTimeout()
 * @method int getRebalanceTimeout()
 * @package EasySwoole\Kafka\Config
 */
class GroupConfig extends Config
{
    use Singleton;

    protected static $defaults = [
        'groupId'          => '',
        'sessionTimeout'   => 30000,
        'rebalanceTimeout' => 30000,
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

    /**
     * @param int $sessionTimeout
     * @throws Exception\Config
     */
    public function setSessionTimeout(int $sessionTimeout): void
    {
        if ($sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['sessionTimeout'] = $sessionTimeout;
    }

    /**
     * @param int $rebalanceTimeout
     * @throws Exception\Config
     */
    public function setRebalanceTimeout(int $rebalanceTimeout): void
    {
        if ($rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['rebalanceTimeout'] = $rebalanceTimeout;
    }
}
