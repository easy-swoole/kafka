<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/19
 * Time: 上午8:58
 */
namespace EasySwoole\Kafka\Exception;


final class InvalidRecordInSet extends Exception
{
    public static function missingTopic(): self
    {
        return new self("You have to set 'topic' to your message.");
    }

    public static function nonExistingTopic(string $topic): self
    {
        return new self(sprintf('Requested topic "%s" does not exist.', $topic));
    }

    public static function topicIsNotString(): self
    {
        return new self('Topic must be string.');
    }

    public static function missingValue(): self
    {
        return new self('You have to set "value" to your message.');
    }

    public static function valueIsNotString(): self
    {
        return new self('Value must be string');
    }
}
