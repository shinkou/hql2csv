# Hive Query Load 2 CSV

A Hive query executer which loads data and saves it to CSVs.

## How To Use

### Prerequisites

A SASL framework and a few common mechanisms are needed.  On Debian based
distros, make sure these packages are installed:

- libsasl2-dev
- libsasl2-modules

### Install

Check out the repo with git.

```
$ git clone https://github.com/shinkou/hql2csv.git
```

Install with pip3 under the checked out directory.

```
$ pip3 install -r requirements.txt
```

### Run

```
$ ./hql2csv.py [ -h | [ OPTIONS ] [ QUERY [ QUERY [ ... ] ] ]
```

| Option         | Description                                               |
|----------------|-----------------------------------------------------------|
| -o, --output   | filepath of the output CSV (default: "output.csv")        |
| -H, --host     | host of the endpoint (default: "localhost")               |
| -P, --port     | port of the endpoint (default: 10000)                     |
| -D, --database | database to connect to (default: "default")               |
| -u, --username | name of the connection user (optional)                    |
| -p, --password | connection passord (optional)                             |
| -A, --auth     | authentication method, "LDAP" or "CUSTOM" only (optional) |
| --poll         | status polling interval in seconds (default: 5)           |
| -h             | print help                                                |
