<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午4:10
 */
namespace EasySwoole\Kafka\Producer;

class Process
{
    /**
     * @var callable|null
     */
    protected $producer;

    /**
     * @var RecordValidator|null
     */
    private $recordValidator;

    public function __construct(?callable $producer = null, ?RecordValidator $recordValidator = null)
    {
        $this->producer         = $producer;
        $this->recordValidator  = $recordValidator ?? new RecordValidator();
    }
}