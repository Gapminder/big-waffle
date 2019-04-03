# Use an official NodeJS runtime to install dependencies
FROM node:alpine as builder

## Install build toolchain, install node deps and compile native add-ons
RUN apk add --no-cache --virtual .gyp python make g++
COPY package.json ./
RUN npm install

# Use an official NodeJS runtime as a parent image
FROM node:alpine as app

# Create app directory
WORKDIR /usr/bw

## Copy built node modules and binaries without including the toolchain
COPY --from=builder node_modules ./node_modules

# Bundle app source and test files
COPY package.json ./
COPY src ./src
COPY test ./test

## Install MariaDB
RUN apk add --no-cache libc6-compat mariadb mariadb-client
RUN mysql_install_db --user=mysql --basedir=/usr --datadir=/var/lib/mysql
RUN mkdir /run/mysqld

## Prepare entry point script
RUN chmod a+x test/*.sh
RUN ln -s /bin/busybox ash

## Environment
ENV ASSET_STORE=local
ENTRYPOINT [ "./test/docker-test.sh" ]
