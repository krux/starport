# Starport

[![Build Status](https://travis-ci.org/krux/starport.svg?branch=master)](https://travis-ci.org/krux/starport)

> The starport is an advanced terran structure, responsible for the
> construction and maintenance of all space-faring vehicles and starships,
> including the Behemoth-class battlecruiser -- Hyperion.

Starport is the service that manages all
[hyperion](https://github.com/krux/hyperion) implemented aws data pipelines.

## Setup

1. Download the latest starport binary [here](https://github.com/krux/starport/releases)
1. Based on the
  [config template](https://github.com/krux/starport/blob/master/conf-template/starport.conf),
  build and deploy the configurations to AWS S3 (e.g.
  `s3://your-bucket/starport.conf`).
1. Initialize the database with the following command:
    ```sh
    java -cp starport-core.jar \
      -Dstarport.config.url=s3://your-bucket/starport.conf \
      com.krux.starport.db.tool.CreateTables true
    ```
