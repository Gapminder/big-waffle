# BigWaffle

BigWaffle is a nodejs web application that can serve DDF Datasets. It is accompanied by a simple command line tool to load and manage datasets.

## Contents

- [How it works](#how-it-works)
- [Installation](#installation)
- [Usage](#usage)
- [Limitations](#limitations-and-known-issues)
- [Credits](#credits)
- [License](#license)
- [Tests](#tests)
- [Contributing](#contributing)

## How it works

BigWaffle host DDF datsets as a tables in a MariaDb database. There are really only four parts to it:
1. Code to load DDF datasets (that have to be in DDFCSV format) into SQL tables.
2. Code that translates DDF queries into the corresponding SQL queries.
3. A command line interface (CLI) that is used to load and otherwise manage datasets.
4. A HTTP service that wraps the code to query datasets and implements the [DDF Service HTTP Protocol Specification](SERVICE_SPEC.md)

### Datasets into tables
BigWaffle uses only a few tables to host a single DDF dataset, even if that set has hundreds or even thousands of indicators. It loads all concepts in one table, creates one table for each entity __domain__, and a datapoints table for each DDF key where e.g. ['geo', 'time'] is one key and ['age', 'geo', 'time'] is a second key. 

The philosophy is that with few (but large) tables the chances that the indexes will (all) fit in memory are pretty high. For example a dataset with two hundred indicators for a single key, and with data for ten thousand points (e.g. 100 years in 100 countries) will need only one index on country to effciently dig out country specific data. If all indicators would have their own table, two hundred indexes would exist and it's less likely that the one needed to satisfy a query would be in memory.

The tables are named with the dataset name and a _version_, making it straightforward to issue queries against specific versions of a dataset.

As SQL is "typed", BigWaffle needs to "analyze" data before it can create an SQL table to store that date into. Now the data in DDFCSV datasets exists in a large number of files, defined in the ```datapackage.json``` file. So loading data consist of two phases: first all datafiles are "scanned", to determine the type and size of columns for the table; then the table is created and the data is inserted into the table, one file at a time. Even on a fast server this will typically take minutes, and for huge datasets with thousands of indicators it can take an hour or more. To keep the process relatively speedy the data (files) have to exist on the server that runs the dataloading code (the CLI), and ideally this machine also hosts the database.

### Processing DDF Queries

DDF Queries are translated into SQL queries. Because of SQL the code for it may look complex, but it is relatively straightforward. The actual SQL query agains the database is then peformed such that the result is a stream, which is then piped (over HTTP) to the client. This way there is no need to hold the complete result set (which can be very large) in (server) memory. Of course it also means that little or no computations (transformations) can be done over multiple rows. But single rows can be, and are, transformed and filtered.

### Translations

Translations, if present in the dataset, are stored in additional columns right next to the columns for the corresponding default language. For example if the default language is English, ```en-UK```, and Finnish (```fi-FI```) translations for the ```name``` of concepts would be available the concepts table would have the following __three__ columns: 
- ```name```, for the default, English, names
- ```_name_fi-FI```, for the Finnish names, with NULL for names for which no Finnish translation was given
- ```name_fi-FI``` a virtual column that takes values from ```_name_fi-FI``` if present (not ```NULL```) and otherwise from ```name```.
DDF queries that request Finnish results would now use the ```name_fi-FI``` column which gives Finnish names if possible but falls back to English names when translations are missing.

### Assets

DDF datasets may have _assets_, files that each are a value of some key in the dataset. These are used to keep very large values out of the dataset proper. Images or large vector maps are typical examples.

BigWaffle is designed to move assets to a cloud storage service such as Google Cloud Storage or Amazon S3. Currently only Google Cloud Storage is supported. 

Within the cloud storage (bucket) assets are saved in a folder structure with a folder for each dataset and a subfolder for each version.

## Installation

BigWaffle was designed to use a MariaDb database service, instructions on the MariaDb installation and configuration are given below.

To install BigWaffle itself simply use ```npm install ...```

To run BigWaffle: ```node src/server.js```

As BigWaffle is intended to run as a service you probably want to setup an init script, or use supervisor or monit.
BigWaffle is configured by environment variables; so you can set those in your init script, supervisor config, or monit control file. The enviroment variables are:

- ```DB_HOST```: the ip address or name of the machine where the MariaDB is. Defaults to ```localhost```.
- ```DB_USER```: the username for access to the database, defaults to ```__USER__``` which is a special value instructing BigWaffle to use the (Unix) username of the currently logged in user (using socket authentication). 
- ```DB_PWD```: the password for the database user
- ```LOG_LEVEL```: the minimum level to log, defaults to ```info```. Possible values are ```trace```, ```debug```, ```info```, ```warn```, ```error```, ```fatal```.
- ```EXTERNAL_LOG```, to give the relative name of a module that will send log entries somewhere else. Defaults to ```none```, but can be set to e.g. ```log-google```. Note that such modules may specify or require additional environment variables to be set.
- ```HTTP_PORT```: the port that the HTTP service will listen on, defaults to `80`
- ```CACHE_ALLOW```: if set to "FALSE" the server will indicate to client to not cache DDF query results. The default is "TRUE". Setting this to "FALSE" may be useful in testing and debugging.
- ```DB_SOCKET_PATH```: the path to the MariaDb socket used by clients. Needed if you want to use Unix socket authentication in MariaDb.
- ```SLACK_CHANNEL_URL```: the URL of a [Slack incoming webhook](https://api.slack.com/incoming-webhooks#posting_with_webhooks). If present the CLI will send notifications to Slack about datasets being loaded.
- ```CPU_THROTTLE```: the number of milliseconds that a service thread can be busy before it responds with a 503. Set this to 0 to disable the check. Defaults to 200 ms.
- ```DB_THROTTLE```: the number of queries that can be pending (waiting for a DB connection) before the service responds with 503. Defaults to 10, set to 0 to disable the check.

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

Use MariaDb version 10.3 or newer. Install CONNECT plugin. Create a DB, and a "user" with all permissions to that db, and one with only "read" access. The web application should be configured to use the later whereas the command line utiltiy will need to more extensive permissions.

Example script to set up the db and users:

### Cloud Deployment

The [Dockerfile](Dockerfile) can be used to create a Docker image that will run the NodeJS service. Note that the image will not include the database.
To build the image issue, in a shell in this directory issue:

    docker build -t big-waffle-server .

Then to run ensure that the relevant environment variables are passed on to the container, e.g. like:

    docker run -e ASSET_STORE_BUCKET='bucket-name' -e DB_HOST=host.docker.internal -e DB_PWD='password' -d -p 80:8888 big-waffle-server:latest


## Usage

See the [DDF HTTP Service doc](SERVICE_SPEC.md) for the full specification of the HTTP(S) interface. If you create visualizations with Vizabi, you can use the [Vizabi BigWaffleReader](https://www.npmjs.com/package/vizabi-ddfservice-reader) to handle the communication with a BigWaffle server.

To load and manage datasets you will use the BigWaffle CLI. Currently the only way to load a dataset is to import a DDFcsv dataset. The [Open Numbers project](https://open-numbers.github.io/) has many such datasets.
These are the typical steps:
`git clone ....`
`npm src/cli.js load -d ddf-gapminder-systema_globalis systema_globalis`

## Slack API
`/bwlist [<dataset>]`  
List all versions of all datasets. Provide a dataset name to see all versions of (only) that dataset.  
Example: `/bwlist SG`  
  
`/bwload [[-N | --name] <name>] [--publish] [-D | --dateversion] [--ddfdir <ddfdirectory>] *<gitCloneUrl>* [<branch>]`  
Load (a new version of) a dataset into BigWaffle. This can take 1-60 minutes!  
Example:  `/bwload -N SG https://github.com/open-numbers/ddf--gapminder--systema_globalis.git`  

`/bwpublish <dataset>`  
Publish the most recently loaded version of a dataset. This unsets any default version, which means that the most recent version will be served by default.  
Example: `/bwpublish SG`  

`/bwdefault <dataset> <version>`  
Make a given version of a dataset the default version. Use this to “revert” to an earlier version.  
Example: `/bwdefault SG 2019032501`  
  
`/bwpurge <dataset>`  
Remove old versions of a given dataset. This will remove versions that were loaded before the current default version, except the version loaded right before the current default.  
Example: `/bwpurge SG`  
  
## Limitations and Known Issues

### "Wide" datasets

BigWaffle maps DDF datasets to a set of SQL tables. MariaDB has some limitations on tables that affect BigWaffle, notably a limitation on the maximum number of columns in a table. For datapoints BigWaffle manages that limitation, it transparantly uses as few as possible but as many as needed tables to host the datapoints. So it's possible to have thousands of indicators in a dataset. Up to approx. 10 000. However, BigWaffle does not (yet) apply the same approach to other parts of the dataset, i.e. not to concepts or entities. This will hardly ever be a problem, but if some entities or concepts have many, many, properties __and__ many [translations](#translations) for many properties, it is possible to hit that limit and encounter errors during the loading of the dataset.

## Credits

See the package.json for all the fine libraries that are used in BigWaffle.

## License

## Tests

The test directory has scripts and data for regression tests. These are full end-to-end tests so can only be run with a properly setup, local!, MariaDb. Also an asset store should be set up. Running the test will then require proper setup of the shell environment. Typically with at least `ASSET_STORE_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS`, and `DB_SOCKET_PATH`, or `DB_PWD` (and `DB_USER`).

Test scripts can be run from npm: ```npm run test-cli``` and ```npm run test-service```.

## Contributing

Raise issues, fork and send us pull requests. Please don't send pull requests without referring to an issue, thank you!
