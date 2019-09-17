<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 下午9:08
 */
namespace EasySwoole\Kafka;

use EasySwoole\Component\Singleton;
use EasySwoole\Spl\SplBean;

class ClientConfig extends SplBean
{
    use Singleton;

    /**
     * @var bool
     */
    protected $open_length_check;

    /**
     * @var string
     */
    protected $package_eof;

    /**
     * @var string
     */
    protected $package_length_type;

    /**
     * @var int
     */
    protected $package_length_offset;

    /**
     * @var int
     */
    protected $package_body_offset;

    /**
     * @var int
     */
    protected $package_max_length;

    /**
     * @var int
     */
    protected $socket_buffer_size;

    /**
     * @var resource
     */
    protected $ssl_cert_file;

    /**
     * @var resource
     */
    protected $ssl_key_file;

    /**
     * @var string
     */
    protected $bind_address;

    /**
     * @var int
     */
    protected $bind_port;

    /**
     * @var bool
     */
    protected $ssl_verify_peer;

    /**
     * @var bool
     */
    protected $ssl_allow_self_signed;

    /**
     * @var resource
     */
    protected $ssl_cafile;

    /**
     * @var resource
     */
    protected $ssl_capath;

    /**
     * @return bool
     */
    public function isOpenLengthCheck (): bool
    {
        return $this->open_length_check;
    }

    /**
     * @param bool $open_length_check
     */
    public function setOpenLengthCheck (bool $open_length_check): void
    {
        $this->open_length_check = $open_length_check;
    }

    /**
     * @return string
     */
    public function getPackageEof (): string
    {
        return $this->package_eof;
    }

    /**
     * @param string $package_eof
     */
    public function setPackageEof (string $package_eof): void
    {
        $this->package_eof = $package_eof;
    }

    /**
     * @return string
     */
    public function getPackageLengthType (): string
    {
        return $this->package_length_type;
    }

    /**
     * @param string $package_length_type
     */
    public function setPackageLengthType (string $package_length_type): void
    {
        $this->package_length_type = $package_length_type;
    }

    /**
     * @return int
     */
    public function getPackageLengthOffset (): int
    {
        return $this->package_length_offset;
    }

    /**
     * @param int $package_length_offset
     */
    public function setPackageLengthOffset (int $package_length_offset): void
    {
        $this->package_length_offset = $package_length_offset;
    }

    /**
     * @return int
     */
    public function getPackageBodyOffset (): int
    {
        return $this->package_body_offset;
    }

    /**
     * @param int $package_body_offset
     */
    public function setPackageBodyOffset (int $package_body_offset): void
    {
        $this->package_body_offset = $package_body_offset;
    }

    /**
     * @return int
     */
    public function getPackageMaxLength (): int
    {
        return $this->package_max_length;
    }

    /**
     * @param int $package_max_length
     */
    public function setPackageMaxLength (int $package_max_length): void
    {
        $this->package_max_length = $package_max_length;
    }

    /**
     * @return int
     */
    public function getSocketBufferSize (): int
    {
        return $this->socket_buffer_size;
    }

    /**
     * @param int $socket_buffer_size
     */
    public function setSocketBufferSize (int $socket_buffer_size): void
    {
        $this->socket_buffer_size = $socket_buffer_size;
    }

    /**
     * @return resource
     */
    public function getSslCertFile (): resource
    {
        return $this->ssl_cert_file;
    }

    /**
     * @param resource $ssl_cert_file
     */
    public function setSslCertFile (resource $ssl_cert_file): void
    {
        $this->ssl_cert_file = $ssl_cert_file;
    }

    /**
     * @return resource
     */
    public function getSslKeyFile (): resource
    {
        return $this->ssl_key_file;
    }

    /**
     * @param resource $ssl_key_file
     */
    public function setSslKeyFile (resource $ssl_key_file): void
    {
        $this->ssl_key_file = $ssl_key_file;
    }

    /**
     * @return string
     */
    public function getBindAddress (): string
    {
        return $this->bind_address;
    }

    /**
     * @param string $bind_address
     */
    public function setBindAddress (string $bind_address): void
    {
        $this->bind_address = $bind_address;
    }

    /**
     * @return int
     */
    public function getBindPort (): int
    {
        return $this->bind_port;
    }

    /**
     * @param int $bind_port
     */
    public function setBindPort (int $bind_port): void
    {
        $this->bind_port = $bind_port;
    }

    /**
     * @return bool
     */
    public function isSslVerifyPeer (): bool
    {
        return $this->ssl_verify_peer;
    }

    /**
     * @param bool $ssl_verify_peer
     */
    public function setSslVerifyPeer (bool $ssl_verify_peer): void
    {
        $this->ssl_verify_peer = $ssl_verify_peer;
    }

    /**
     * @return bool
     */
    public function isSslAllowSelfSigned (): bool
    {
        return $this->ssl_allow_self_signed;
    }

    /**
     * @param bool $ssl_allow_self_signed
     */
    public function setSslAllowSelfSigned (bool $ssl_allow_self_signed): void
    {
        $this->ssl_allow_self_signed = $ssl_allow_self_signed;
    }

    /**
     * @return resource
     */
    public function getSslCafile (): resource
    {
        return $this->ssl_cafile;
    }

    /**
     * @param resource $ssl_cafile
     */
    public function setSslCafile (resource $ssl_cafile): void
    {
        $this->ssl_cafile = $ssl_cafile;
    }

    /**
     * @return resource
     */
    public function getSslCapath (): resource
    {
        return $this->ssl_capath;
    }

    /**
     * @param resource $ssl_capath
     */
    public function setSslCapath (resource $ssl_capath): void
    {
        $this->ssl_capath = $ssl_capath;
    }

    public function __construct (array $data = null, bool $autoCreateProperty = false)
    {
        // 给默认初始值
        $this->setOpenLengthCheck(1);
        $this->setPackageLengthType('N');
        $this->setPackageLengthOffset(0);
        $this->setPackageBodyOffset(4);
        $this->setPackageMaxLength(2000000);

        parent::__construct($data, $autoCreateProperty);
    }

}