<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\DescribeGroups;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class DescribeGroupsTest extends TestCase
{
    /**
     * @var DescribeGroups
     */
    private $describe;

    public function setUp(): void
    {
        $this->describe = new DescribeGroups('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function testEncode(): void
    {
        $data = ['test'];

        $test = $this->describe->encode($data);
        self::assertSame('00000024000f00000000000f00104561737973776f6f6c652d6b61666b6100000001000474657374', bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function testEncodeEmptyArray(): void
    {
        $data = [];

        $test = $this->describe->encode($data);
        self::assertSame('0000001e000f00000000000f00104561737973776f6f6c652d6b61666b6100000000', bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '0000000100000004746573740004446561640000000000000000';
        $expected = '[{"errorCode":0,"groupId":"test","state":"Dead","protocolType":"","protocol":"","members":[]}]';

        $test = $this->describe->decode(hex2bin($data));
        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
