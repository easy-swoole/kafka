<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午3:57
 */
namespace EasySwoole\Kafka\Producer;

use EasySwoole\Kafka\Exception;

final class RecordValidator
{
    /**
     * @param array $record
     * @param array $topicList
     * @throws Exception\InvalidRecordInSet
     */
    public function validate(array $record, array $topicList): void
    {
        if (! isset($record['topic'])) {
            throw Exception\InvalidRecordInSet::missingTopic();
        }

        if (! is_string($record['topic'])) {
            throw Exception\InvalidRecordInSet::topicIsNotString();
        }

        if (trim($record['topic']) === '') {
            throw Exception\InvalidRecordInSet::missingTopic();
        }

        if (! isset($topicList[$record['topic']])) {
            throw Exception\InvalidRecordInSet::nonExistingTopic($record['topic']);
        }

        if (! isset($record['value'])) {
            throw Exception\InvalidRecordInSet::missingValue();
        }

        if (! is_string($record['value'])) {
            throw Exception\InvalidRecordInSet::valueIsNotString();
        }

        if (trim($record['value']) === '') {
            throw Exception\InvalidRecordInSet::missingValue();
        }
    }
}
