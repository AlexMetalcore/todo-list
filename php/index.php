<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use RdKafka\Conf;
use RdKafka\KafkaConsumer;

$kafkaBrokers = getenv('KAFKA_BROKERS');
$topic = getenv('KAFKA_TOPIC');
$group = getenv('KAFKA_GROUP');

$pgDsn = sprintf('pgsql:host=%s;port=%s;dbname=%s',
    getenv('PG_HOST'),
    getenv('PG_PORT'),
    getenv('PG_DB')
);
$pgUser = getenv('PG_USER');
$pgPass = getenv('PG_PASSWORD');


$pdo = new PDO($pgDsn, $pgUser, $pgPass, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$pdo->exec(
    'CREATE TABLE IF NOT EXISTS posts_inbox (
                id BIGSERIAL PRIMARY KEY,
                source_id BIGINT UNIQUE,
                email VARCHAR(255) NOT NULL,
                username TEXT NULL,
                content TEXT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW()
            )'
);

$pdo->exec('CREATE INDEX IF NOT EXISTS posts_inbox_inbox_source_id ON posts_inbox (source_id)');

$conf = new Conf();
$conf->set('bootstrap.servers', $kafkaBrokers);
$conf->set('group.id', $group);
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'earliest');

$consumer = new KafkaConsumer($conf);
$consumer->subscribe([$topic]);

echo "PHP consumer started. Listening on topic $topic...\n";

while (true) {
    $message = $consumer->consume(120 * 1000); // 120s
    if ($message === null) {
        continue;
    }

    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            try {
                $posts = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

                if (($posts['event'] ?? '') !== 'post.created') {
                    echo "Skip unknown event\n";
                    break;
                }

                $postsId = (int)($posts['id'] ?? 0);
                $email = (string)($posts['payload']['email'] ?? '');
                $username = (string)($posts['payload']['username'] ?? '');
                $content = (string)($posts['payload']['content'] ?? '');

                $statement = $pdo->prepare(
                    'INSERT INTO posts_inbox (source_id, email, username, content)
                            VALUES (:source_id, :email, :username, :content)'
                );
                $statement->execute([
                    ':source_id' => $postsId,
                    ':email' => $email,
                    ':username' => $username,
                    ':content'=> $content,
                ]);
                $consumer->commit($message);
                
                echo "Saved todo $postsId\n";
            } catch (Throwable $e) {
                fwrite(STDERR, "Error: {$e->getMessage()}\n");
            }
            break;
            
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            break; // достигнут конец партиции
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Timed out...\n";
            break;
        default:
            fwrite(STDERR, "Kafka error: {$message->errstr()}\n");
            break;
    }
}