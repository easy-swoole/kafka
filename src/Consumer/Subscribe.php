<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/27
 * Time: 下午4:11
 */
namespace EasySwoole\Kafka\Consumer;

use EasySwoole\Kafka\BaseProcess;

class Subscribe extends BaseProcess
{

    protected $isAutoCommit = false;

    private $partitions = [];

    public function setAutoCommit(bool $isAuto)
    {
        $this->isAutoCommit = $isAuto;
    }

    public function fetchMsg(float $timeout = 3.0)
    {


        $this->setPartition();
    }

    public function fetchOffset(float $timeout = 3.0)
    {

        $partitions = '';
        $this->setPartition($partitions);
    }

    public function commit(Msg $msg)
    {

        $this->partitions[$msg->partition] = $msg->offset + 1;
    }



    public function subscribe(callable $func, float $timeout = 3.0)
    {
        while (1) {
            $msg = $this->fetchMsg($timeout);
            if ($msg) {
                $ret = call_user_func($func, $msg);
                if ($this->isAutoCommit && $ret) {
                    $this->commit();
                }
            }
        }
    }

    public function setPartition($partitions)
    {
        if (is_string($partitions)) {
            $this->partitions[$partitions] = 0;
        } else {
            foreach ($partitions as $partition => $offset) {
                $this->partitions[$partition] = $offset;
            }
        }
    }
}
