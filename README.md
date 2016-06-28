# glacier-client

[![Build Status](https://travis-ci.org/marko-asplund/glacier-client.svg?branch=master)](https://travis-ci.org/marko-asplund/glacier-client)

Amazon Glacier backup tool with the following features
- vault management: create, describe, list, delete
- archive management: upload, delete, download
- local catalog management: add, list, delete, synchronize with Glacier inventory

glacier-client can be used through its API in Scala programs or as an interactive backup CLI (via REPL).
