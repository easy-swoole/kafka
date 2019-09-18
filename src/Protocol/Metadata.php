<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/27
 * Time: 上午9:25
 */
namespace EasySwoole\Kafka\Protocol;

use EasySwoole\Kafka\Exception\Protocol as ProtocolException;

class Metadata extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws ProtocolException
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function encode(array $payloads = []): string
    {
        foreach ($payloads as $topic) {
            if (! is_string($topic)) {
                throw new ProtocolException(
                    "request metadata topic array have invalid value."
                );
            }
        }

        $header = $this->requestHeader('Easyswoole-kafka', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray($payloads, [$this, 'encodeString'], self::PACK_INT16);
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
        $offset       = 0;
        $brokerRet    = $this->decodeArray(substr($data, $offset), [$this, 'metaBroker']);
        $offset      += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), [$this, 'metaTopicMetaData']);
        $offset      += $topicMetaRet['length'];

        $result = [
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        ];
        return $result;
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function metaBroker(string $data): array
    {
        $offset       = 0;
        // int32
        $nodeId       = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset      += 4;
        // string 前2个字节是长度
        $hostNameInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $hostNameInfo['length'];
        $port         = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset      += 4;

        return [
            'length' => $offset,
            'data'   => [
                'host'   => $hostNameInfo['data'],
                'port'   => $port,
                'nodeId' => $nodeId,
            ],
        ];
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function metaTopicMetaData(string $data): array
    {
        $offset         = 0;
        $topicErrCode   = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset        += 2;
        $topicInfo      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset        += $topicInfo['length'];
        $partitionsMeta = $this->decodeArray(substr($data, $offset), [$this, 'metaPartitionMetaData']);
        $offset        += $partitionsMeta['length'];

        return [
            'length' => $offset,
            'data'   => [
                'topicName'  => $topicInfo['data'],
                'errorCode'  => $topicErrCode,
                'partitions' => $partitionsMeta['data'],
            ],
        ];
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function metaPartitionMetaData(string $data): array
    {
        $offset   = 0;
        $errcode  = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset  += 2;
        $partId   = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset  += 4;
        $leader   = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset  += 4;
        $replicas = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset  += $replicas['length'];
        $isr      = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset  += $isr['length'];

        return [
            'length' => $offset,
            'data'   => [
                'partitionId' => $partId,
                'errorCode'   => $errcode,
                'replicas'    => $replicas['data'],
                'leader'      => $leader,
                'isr'         => $isr['data'],
            ],
        ];
    }
}
