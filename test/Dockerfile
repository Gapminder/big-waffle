# Use an official NodeJS runtime to install dependencies
FROM node:lts-alpine as builder

## Install build toolchain, install node deps and compile native add-ons
RUN apk add --no-cache --virtual .gyp python make g++
COPY package.json ./
RUN npm install

# Use an official NodeJS runtime as a parent image.
# NOTE: using alpine3.10 as newer alpine versions pull in mariadb 10.4 which leads to a problem with the authentication module.
FROM node:lts-alpine3.10 as app

# Create app directory
WORKDIR /usr/bw

## Copy built node modules and binaries without including the toolchain
COPY --from=builder node_modules ./node_modules

# Bundle app source and test files
COPY package.json ./
COPY src ./src
COPY test ./test

## Install MariaDB
RUN apk add --no-cache libc6-compat 'mariadb<10.4' 'mariadb-client<10.4'
RUN mysql_install_db --auth-root-authentication-method=normal --user=mysql --basedir=/usr --datadir=/var/lib/mysql
RUN mkdir /run/mysqld

## Prepare entry point script
RUN chmod a+x test/*.sh
RUN ln -s /bin/busybox ash

## Environment
ENV ASSET_STORE=local
ENTRYPOINT [ "./test/docker-test.sh" ]
