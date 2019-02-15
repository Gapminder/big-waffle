# BigWaffle

BigWaffle is a nodejs web application that can serve DDF Datasets. It is accompanied by a simple command line tool to load and manage datasets.

## Installation

BigWaffle was designed to use a MariaDb database service, instructions on the MariaDb installation and configuration are given below.

To install BigWaffle itself simply use ```npm install ...```

To run BigWaffle: ```node src/server.js```

As BigWaffle is intended to run as a service you probably want to setup an init script, or use supervisor or monit.
BigWaffle is configured by environment variables; so you can set those in your init script, supervisor config, or monit control file. The enviroment variables are:

```HTTP_PORT```: the port that the HTTP service will listen on, defaults to `80`

### Installation and configuration of MariaDb

Use MariaDb version 10.3 or newer. Install CONNECT plugin. Create a DB, and a "user" with all permissions to that db, and one with only "read" access. The web application should be configures to use the later whereas the command line utiltiy will need to more extensive permissions.

Example script to set up the db and users:


## Usage

See the DDF HTTP Service doc for the full specification of the HTTP(S) interface. If you create visualizations with Vizabi, you can use the Vizabi BigWaffleReader to handle the communication with a BigWaffle server.

To load and manage datasets you will use the BigWaffle CLI. Currently the only way to load a dataset is to import a DDFcsv dataset. The Open Numbers project has many such datasets.
These are the typical steps:
`git clone ....`
`npm src/cli.js load -d ddf-gapminder-systema_globalis systema_globalis`

## Credits

See the package.json for all the fine libraries that are used in BigWaffle.

## License

## Contributing

Raise issues, fork and send us pull requests. Please don't send pull requests without referring to an issue, thank you!
