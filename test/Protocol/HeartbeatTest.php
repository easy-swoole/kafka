<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\Heartbeat;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class HeartbeatTest extends TestCase
{
    /**
     * @var Heartbeat
     */
    private $heart;

    public function setUp(): void
    {
        $this->heart = new Heartbeat('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'group_id'      => 'test',
            'member_id'     => 'EasySwoole-kafka-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        ];

        $expected = '0000005b000c00000000000c00104561737973776f6f6c652d6b61666b610004746573740000000200354561737953776f6f6c652d6b61666b612d30653763626433332d373935302d343061662d623639312d656365616136363564323937';
        $test     = $this->heart->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $this->heart->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGenerationId(): void
    {
        $data = ['group_id' => 'test'];

        $this->heart->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMemberId(): void
    {
        $data = [
            'group_id'      => 'test',
            'generation_id' => '1',
        ];

        $this->heart->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $test     = $this->heart->decode(hex2bin('0000'));
        $expected = '{"errorCode":0}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
