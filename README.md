# BigWaffle

BigWaffle is a nodejs web application that can serve DDF Datasets. It is accompanied by a simple command line tool to load and manage datasets.

## Installation

BigWaffle was designed to use a MariaDb database service, instructions on the MariaDb installation and configuration are given below.

To install BigWaffle itself simply use ```npm install ...```

To run BigWaffle: ```node src/server.js```

As BigWaffle is intended to run as a service you probably want to setup an init script, or use supervisor or monit.
BigWaffle is configured by environment variables; so you can set those in your init script, supervisor config, or monit control file. The enviroment variables are:

```HTTP_PORT```: the port that the HTTP service will listen on, defaults to `80`
```CACHE_ALLOW```: if set to "FALSE" the server will indicate to client to not cache DDF query results. The default is "TRUE". Setting this to "FALSE" may be useful in testing and debugging.
```DB_SOCKET_PATH```: the path to the MariaDb socket used by clients. Needed if you want to use Unix socket authentication in MariaDb.
```SLACK_CHANNEL_URL```: the URL of a [Slack incoming webhook](https://api.slack.com/incoming-webhooks#posting_with_webhooks). If present the CLI will send notifications to Slack about datasets being loaded.
```CPU_THROTTLE```: the number of milliseconds that a service thread can be busy before it responds with a 503. Set this to 0 to disable the check. Defaults to 200 ms.
```DB_THROTTLE```: the number of queries that can be pending (waiting for a DB connection) before the service responds with 503. Defaults to 10, set to 0 to disable the check.

### Setup cloud storage

DDF datasets may have _assets_, files that each are a value of some key in the dataset. These are used to keep very large values out of the dataset proper. Images or large vector maps are typical examples.

BigWaffle is designed to move assets to a cloud storage service such as Google Cloud Storage or Amazon S3. Currently only Google Cloud Storage is supported. 

#### Google Cloud Storage

Setup a bucket on GCS, and make sure to have a _service account_ that can administer the bucket. Download and save the credentials file for that account on the server and make sure it can be read by the server account that will run the service. Then in the environment for the service set the variables:

```GOOGLE_APPLICATION_CREDENTIALS```: the path and name of the credentials file
```ASSET_STORE```: the type of cloud storage to use for dataset assets, defaults to `GCS`(which is the only supported option at the moment) so can also be absent
```ASSET_STORE_BUCKET```: the name of the (root) bucket on the cloud storage service

It is important to have a correct CORS policy on the bucket. [This is an example of how to do that](https://bitmovin.com/docs/encoding/faqs/how-do-i-set-up-cors-for-my-google-cloud-storage-bucket).


### Installation and configuration of MariaDb

Use MariaDb version 10.3 or newer. Install CONNECT plugin. Create a DB, and a "user" with all permissions to that db, and one with only "read" access. The web application should be configures to use the later whereas the command line utiltiy will need to more extensive permissions.

Example script to set up the db and users:

### Cloud Deployment

The [Dockerfile](Dockerfile) can be used to create a Docker image that will run the NodeJS service. Note that the image will not include the database.
To build the image issue, in a shell in this directory issue:

    docker build -t big-waffle-server .

Then to run ensure that the relevant environment variables are passed on to the container, e.g. like:

    docker run -e ASSET_STORE_BUCKET='bucket-name' -e DB_HOST=host.docker.internal -e DB_PWD='password' -d -p 80:8888 big-waffle-server:latest


## Usage

See the DDF HTTP Service doc for the full specification of the HTTP(S) interface. If you create visualizations with Vizabi, you can use the Vizabi BigWaffleReader to handle the communication with a BigWaffle server.

To load and manage datasets you will use the BigWaffle CLI. Currently the only way to load a dataset is to import a DDFcsv dataset. The Open Numbers project has many such datasets.
These are the typical steps:
`git clone ....`
`npm src/cli.js load -d ddf-gapminder-systema_globalis systema_globalis`

## Credits

See the package.json for all the fine libraries that are used in BigWaffle.

## License

## Tests

The test directory has scripts and data for regression tests. These are full end-to-end tests so can only be run with a properly setup, local!, MariaDb. Also an asset store should be set up. Running the test will then require proper setup of the shell environment. Typically with at least `ASSET_STORE_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS`, and `DB_SOCKET_PATH`, or `DB_PWD` (and `DB_USER`).

Test scripts can be run from npm: ```npm run test-cli``` and ```npm run test-service```.

## Contributing

Raise issues, fork and send us pull requests. Please don't send pull requests without referring to an issue, thank you!
