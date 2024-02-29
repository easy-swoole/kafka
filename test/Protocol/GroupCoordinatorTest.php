<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Exception\Protocol as ProtocolException;
use EasySwoole\Kafka\Protocol\GroupCoordinator;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class GroupCoordinatorTest extends TestCase
{
    /**
     * @var GroupCoordinator
     */
    private $group;

    public function setUp(): void
    {
        $this->group = new GroupCoordinator('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = ['group_id' => 'test'];

        $test = $this->group->encode($data);
        self::assertSame('00000020000a00000000000a00104561737973776f6f6c652d6b61666b61000474657374', bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('given group coordinator invalid. `group_id` is undefined.');
        $this->group->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '000000000003000b31302e31332e342e313539000023e8';
        $expected = '{"errorCode":0,"nodeId":3,"coordinatorHost":"10.13.4.159","coordinatorPort":9192}';

        $test = $this->group->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
