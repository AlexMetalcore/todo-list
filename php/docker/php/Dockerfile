FROM php:8.4-cli-alpine

RUN apk add --no-cache $PHPIZE_DEPS linux-headers openssl-dev cyrus-sasl-dev \
    librdkafka-dev postgresql-dev \
 && pecl install rdkafka \
 && docker-php-ext-enable rdkafka \
 && docker-php-ext-install pdo_pgsql 

RUN apk add curl \
&& curl -s https://raw.githubusercontent.com/composer/getcomposer.org/76a7060ccb93902cd7576b67264ad91c8a2700e2/web/installer | php -- --quiet \
&& mv composer.phar /usr/bin/composer

WORKDIR /app
COPY /php/index.php /app/index.php

ENTRYPOINT ["php", "/app/index.php"]