<?php


namespace EasySwoole\Kafka\Protocol;


use EasySwoole\Kafka\Exception\NotSupported;

abstract class AbstractProtocol
{
    /**
     *  Default kafka broker verion
     */
    public const DEFAULT_BROKER_VERION = '0.9.0.0';

    /**
     *  Kafka server protocol version0
     */
    public const API_VERSION0 = 0;

    /**
     *  Kafka server protocol version 1
     */
    public const API_VERSION1 = 1;

    /**
     *  Kafka server protocol version 2
     */
    public const API_VERSION2 = 2;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION0 = 0;

    /**
     * use encode message, This is a version id used to allow backwards
     * compatible evolution of the message binary format.
     */
    public const MESSAGE_MAGIC_VERSION1 = 1;

    /**
     * message no compression
     */
    public const COMPRESSION_NONE = 0;

    /**
     * Message using gzip compression
     */
    public const COMPRESSION_GZIP = 1;

    /**
     * Message using Snappy compression
     */
    public const COMPRESSION_SNAPPY = 2;

    /**
     *  pack int32 type
     */
    public const PACK_INT32 = 0;

    /**
     * pack int16 type
     */
    public const PACK_INT16 = 1;

    /**
     * protocol request code
     */
    public const PRODUCE_REQUEST = 0;

    public const FETCH_REQUEST = 1;

    public const OFFSET_REQUEST = 2;

    public const METADATA_REQUEST = 3;

    public const OFFSET_COMMIT_REQUEST = 8;

    public const OFFSET_FETCH_REQUEST = 9;

    public const GROUP_COORDINATOR_REQUEST = 10;

    public const JOIN_GROUP_REQUEST = 11;

    public const HEART_BEAT_REQUEST = 12;

    public const LEAVE_GROUP_REQUEST = 13;

    public const SYNC_GROUP_REQUEST = 14;

    public const DESCRIBE_GROUPS_REQUEST = 15;

    public const LIST_GROUPS_REQUEST = 16;

    public const SASL_HAND_SHAKE_REQUEST = 17;

    public const API_VERSIONS_REQUEST = 18;

    public const CREATE_TOPICS_REQUEST = 19;

    public const DELETE_TOPICS_REQUEST = 20;

    // unpack/pack bit
    public const BIT_B64 = 'N2';

    public const BIT_B32 = 'N';

    public const BIT_B16 = 'n';

    public const BIT_B16_SIGNED = 's';

    public const BIT_B8 = 'C';

    /**
     * @var string
     */
    protected $version = self::DEFAULT_BROKER_VERION;

    /**
     * Converts a signed short (16 bits) from little endian to big endian.
     *
     * @param int[] $bits
     *
     * @return int[]
     */
    public static function convertSignedShortFromLittleEndianToBigEndian(array $bits): array
    {
        $convert = function (int $bit): int {
            $lsb = $bit & 0xff;
            $msb = $bit >> 8 & 0xff;
            $bit = $lsb << 8 | $msb;

            if ($bit >= 32768) {
                $bit -= 65536;
            }

            return $bit;
        };

        return array_map($convert, $bits);
    }

    public function requestHeader(string $clientId, int $correlationId, int $apiKey): string
    {
        // int16 -- apiKey int16 -- apiVersion int32 correlationId
        $binData = self::pack(self::BIT_B16, (string)$apiKey);
        $binData .= self::pack(self::BIT_B16, (string)$this->getApiVersion($apiKey));
        $binData .= self::pack(self::BIT_B32, (string)$correlationId);
        // concat client id
        $binData .= self::encodeString($clientId, self::PACK_INT16);
        return $binData;
    }

    public static function pack(string $type, string $data): string
    {
        if ($type !== self::BIT_B64) {
            return pack($type, $data);
        }

        if ((int) $data === -1) { // -1L
            return hex2bin('ffffffffffffffff');
        }

        if ((int) $data === -2) { // -2L
            return hex2bin('fffffffffffffffe');
        }

        $left  = 0xffffffff00000000;
        $right = 0x00000000ffffffff;

        $l = ($data & $left) >> 32;
        $r = $data & $right;

        return pack($type, $l, $r);
    }


    /**
     * Get kafka api version according to specify kafka broker version
     */
    public function getApiVersion(int $apiKey): int
    {
        switch ($apiKey) {
            case self::FETCH_REQUEST:
            case self::PRODUCE_REQUEST:{
                if (version_compare($this->version, '0.10.0') >= 0) {
                    return self::API_VERSION2;
                }
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;
            }
            case self::OFFSET_COMMIT_REQUEST:{
                if (version_compare($this->version, '0.9.0') >= 0) {
                    return self::API_VERSION2;
                }
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;
            }
            case self::OFFSET_FETCH_REQUEST:{
                if (version_compare($this->version, '0.8.2') >= 0) {
                    return self::API_VERSION1; // Offset Fetch Request v1 will fetch offset from Kafka
                }
                return self::API_VERSION0;
            }
            case self::JOIN_GROUP_REQUEST:{
                if (version_compare($this->version, '0.10.1.0') >= 0) {
                    return self::API_VERSION1;
                }
                return self::API_VERSION0;
            }
            default:{
                return self::API_VERSION0;
            }
        }
    }

    public static function encodeString(string $string, int $bytes, int $compression = self::COMPRESSION_NONE): string
    {
        $packLen = $bytes === self::PACK_INT32 ? self::BIT_B32 : self::BIT_B16;
        $string  = self::compress($string, $compression);

        return self::pack($packLen, (string) strlen($string)) . $string;
    }

    private static function compress(string $string, int $compression): string
    {
        if ($compression === self::COMPRESSION_NONE) {
            return $string;
        }

        if ($compression === self::COMPRESSION_SNAPPY) {
            throw new NotSupported('SNAPPY compression not yet implemented');
        }

        if ($compression !== self::COMPRESSION_GZIP) {
            throw new NotSupported('Unknown compression flag: ' . $compression);
        }

        return gzencode($string);
    }
}