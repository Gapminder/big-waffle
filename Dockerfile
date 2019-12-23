# Use an official NodeJS runtime as a parent image
FROM node:lts-alpine as builder

## Install build toolchain, install node deps and compile native add-ons
RUN apk add --no-cache --virtual .gyp python make g++
COPY package.json ./
RUN npm install --production

FROM node:lts-alpine as app

RUN apk add --no-cache libc6-compat

# Create app directory
WORKDIR /usr/src/app

## Copy built node modules and binaries without including the toolchain
COPY --from=builder node_modules ./node_modules

# Bundle app source
COPY src ./

# Make port 80 available to the world outside this container
EXPOSE 8888

# Define environment variable
ENV HTTP_PORT=8888

CMD [ "node", "server.js"]