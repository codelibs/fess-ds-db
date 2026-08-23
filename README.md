DB Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-db/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-db/actions/workflows/maven.yml)
==========================

## Overview

DB Data Store is an extension for Fess Data Store Crawling. It crawls the rows
returned by an SQL query over a JDBC connection, turning each row into a
document.

## Installation

Install it from the Fess admin UI, under System > Plugin, or place the jar
manually:

1. Download `fess-ds-db-X.X.X.jar` from the
   [CodeLibs repository](https://maven.codelibs.org/release/org/codelibs/fess/fess-ds-db/).
2. Copy it to `$FESS_HOME/app/WEB-INF/plugin` (`/usr/share/fess/app/WEB-INF/plugin`
   for a package install).

**The JDBC driver for your database is a separate download.** Copy its jar to
`$FESS_HOME/app/WEB-INF/lib` or `$FESS_HOME/app/WEB-INF/env/crawler/lib`, then
restart Fess. Without it the crawl fails with `The JDBC driver ... is not on the
crawler classpath`.

## Documentation

See [Database Data Store](https://fess.codelibs.org/15.8/config/datastore/ds-database.html)
for the parameters and configuration examples.
