<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午3:53
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Group\Process;

class Group
{
    private $process;

    /**
     * Group constructor.
     * @throws Exception\Exception
     */
    public function __construct()
    {
        $this->process = new Process();
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function findCoordinator()
    {
        return $this->process->getGroupBrokerId();
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function joinGroup()
    {
        return $this->process->joinGroup();
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function leaveGroup()
    {
        return $this->process->leaveGroup();
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function listGroup()
    {
        return $this->process->listGroup();
    }


    public function syncGroup()
    {
        return $this->process->syncGroup();
    }
}
