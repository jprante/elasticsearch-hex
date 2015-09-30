# Hex bulk indexing plugin for Elasticsearch

This plugin for Elasticsearch converts JSON hexadecimal encoding (base16) to binary data for Elasticsearch bulk indexing.

It is a customized bulk API endpoint called `_bulkhex` and processes REST HTTP JSON bodies.

The JSON processing uses a custom XContent API.

## Compatibility matrix

| Elasticsearch    | Plugin     | Release date |
| ---------------- | -----------| -------------|
| 1.6.2            | 1.6.2.0    | Sep 30, 2015 |

## Installation

    ./bin/plugin --install hex --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-hex/1.6.2.0/elasticsearch-hex-1.6.2.0-plugin.zip

Do not forget to restart the node after installing.

# Example

See `HexPluginTest.java` for using the API.

    curl -XPOST '0:9200/_bulkhex'
    {"index":{"_index":"test","_type":"test","_id":"1"}
    {"hex":"4AC3B67267","nothex":"Hello HTTP World"}

The indexed document shows the base64 encoding of the decoded base16 text.

    {"hex":"SsO2cmc=","nothex":"Hello HTTP World"}

# License

Elasticsearch Hex Plugin

Copyright (C) 2015 Jörg Prante

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
