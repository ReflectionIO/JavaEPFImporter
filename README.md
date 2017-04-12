Java EPF Importer
=================

A version of apple's EPF importer written in Java. The original scripts can be found at http://www.apple.com/itunes/affiliates/resources/documentation/epfimporter.html#configure-epfimporter.

## Current supported ingesters

- MySQL (original ingestor)
- DataStore using appengine remote api

There is also a fork [here](https://github.com/Cheers-Dev/JavaEPFImporter) that supports Postgres.

## Usage
```bash
java -jar epfimporter.jar [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]
    [-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]]
    [-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]
```

- __f__ or __flat__: Import EPF Flat files, using values from `EPFFlat.config` if not overridden - defaults to false 
- __r__ or __resume__: Resume the most recent import according to the relevant `.json` status file (`EPFStatusIncremental.json` if `-i`, otherwise `EPFStatusFull.json`) - defaults to false
- __z__ or __ingestertype__: Ingester type name, currently the only ones supported are __MySQL__ and __DataStore__
- __d__ or __dbhost__: The hostname of the database - defaults to localhost
- __u__ or __dbuser__: The user which will execute the database commands; must have table create/drop privileges
- __p__ or __dbpassword__: The user's password for the database
- __n__ or __dbname__: The name of the database to connect to
- __s__ or __recordseparator__: The string separating records in the file
- __t__ or __fieldseparator__: The string separating fields in the file
- __a__ or __allowextensions__: Include files with dots in their names in the import - defaults to false
- __x__ or __tableprefix__: Optional prefix which will be added to all table names, e.g. `MyPrefix_video_translation`
- __w__ or __whitelist__: A regular expression to add to the whiteList; repeated `-w` arguments will append
- __b__ or __blacklist__: A regular expression to add to the whiteList; repeated `-b` arguments will append
- __k__ or __skipkeyviolators__: Ignore inserts which would violate a primary key constraint; only applies to full imports - defaults to false


## Contribute
Please feel free to raise issues and contribute fixes, but please note that this is a work in progress.
