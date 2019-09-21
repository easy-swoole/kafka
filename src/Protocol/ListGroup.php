<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午8:46
 */
namespace EasySwoole\Kafka\Protocol;

use EasySwoole\Kafka\Exception\NotSupported;
use function substr;

class ListGroup extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws NotSupported
     */
    public function encode(array $payloads = []): string
    {
        $header = $this->requestHeader('Easyswoole-kafka', self::LIST_GROUPS_REQUEST, self::LIST_GROUPS_REQUEST);

        return self::encodeString($header, self::PACK_INT32);
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function decode(string $data): array
    {
        $offset    = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset   += 2;
        $groups    = $this->decodeArray(substr($data, $offset), [$this, 'decodeGroup']);

        return [
            'errorCode' => $errorCode,
            'groups'    => $groups['data'],
        ];
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function decodeGroup(string $data): array
    {
        $offset       = 0;
        $groupId      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $groupId['length'];
        $protocolType = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $protocolType['length'];

        return [
            'length' => $offset,
            'data'   => [
                'groupId'      => $groupId['data'],
                'protocolType' => $protocolType['data'],
            ],
        ];
    }
}
