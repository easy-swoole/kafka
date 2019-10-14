<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/27
 * Time: 下午4:18
 */
namespace EasySwoole\Kafka\Consumer;

class Msg
{
    /**
     * @var int
     */
    public $offset;

    /**
     * @var int
     */
    public $partition;
}
