<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\ListGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class ListGroupTest extends TestCase
{
    /**
     * @var ListGroup
     */
    private $list;

    public function setUp(): void
    {
        $this->list = new ListGroup('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function testEncode(): void
    {
        $test = $this->list->encode();

        self::assertSame('0000001a001000000000001000104561737973776f6f6c652d6b61666b61', bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $test     = $this->list->decode(hex2bin('0000000000010004746573740008636f6e73756d6572'));
        $expected = '{"errorCode":0,"groups":[{"groupId":"test","protocolType":"consumer"}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
