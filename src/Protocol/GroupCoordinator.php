<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午4:07
 */
namespace EasySwoole\Kafka\Protocol;

use EasySwoole\Kafka\Exception\NotSupported;
use EasySwoole\Kafka\Exception\Protocol as ProtocolException;
use function substr;

class GroupCoordinator extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given group coordinator invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('Easyswoole-kafka', self::GROUP_COORDINATOR_REQUEST, self::GROUP_COORDINATOR_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function decode(string $data): array
    {
        $offset          = 0;
        $errorCode       = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));// errorcCode int16
        $offset         += 2;
        $nodeId          = self::unpack(self::BIT_B32, substr($data, $offset, 4));// node_id int32
        $offset         += 4;
        $hosts           = $this->decodeString(substr($data, $offset), self::BIT_B16);// host string
        $offset         += $hosts['length'];
        $coordinatorPort = self::unpack(self::BIT_B32, substr($data, $offset, 4));// port int32
        $offset         += 4;

        return [
            'errorCode'       => $errorCode,
            'nodeId'          => $nodeId,
            'coordinatorHost' => $hosts['data'],
            'coordinatorPort' => $coordinatorPort,
        ];
    }
}
