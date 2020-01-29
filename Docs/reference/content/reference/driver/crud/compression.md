+++
date = "2020-01-29T22:05:03-04:00"
title = "Compression"
[menu.main]
  parent = "Reference Reading and Writing"
  identifier = "Compression"
  weight = 25
  pre = "<i class='fa'></i>"
+++

## Compression

The C# driver supports compression of messages to and from MongoDB servers. The driver implements the three algorithms that are supported by MongoDB servers:

* [Snappy](https://google.github.io/snappy/): Snappy compression can be used when connecting to MongoDB servers starting with the 3.4 release.
* [Zlib](https://zlib.net/): Zlib compression can be used when connecting to MongoDB servers starting with the 3.6 release.
* [Zstandard](https://facebook.github.io/zstd/): Zstandard compression can be used when connecting to MongoDB servers starting with the 4.2 release.

The driver will negotiate which, if any, compression algorithm is used based on capabilities advertised by the server in the [ismaster]({{<docsref "reference/command/isMaster/">}}) command response. 

### Specify compression via `URI`

```c#
using MongoDB.Driver;
```

To specify compression with [`ConnectionString`]({{<apiref "T_MongoDB_Driver_Core_Configuration_ConnectionString">}}), just add `compressors` into the connection string, as in:

```c#
var mongoUrl = new MongoUrl("mongodb://localhost/?compressors=snappy");
var client = new MongoClient(mongoUrl);
```
for Snappy compression, or

```c#
var mongoUrl = new MongoUrl("mongodb://localhost/?compressors=zlib");
var client = new MongoClient(mongoUrl);
```
for zlib compression, or 

```c#
var mongoUrl = new MongoUrl("mongodb://localhost/?compressors=zstd");
var client = new MongoClient(mongoUrl);
```
for Zstandard compression, or 

```c#
var mongoUrl = new MongoUrl("mongodb://localhost/?compressors=snappy,zlib,zstd");
var client = new MongoClient(mongoUrl);
```
to configure multiple compressors.

In all cases the driver will use the first compressor in the list for which the server advertises support. 

### Specify compression via `MongoClientSettings`

```c#
using MongoDB.Driver;
using MongoDB.Driver.Core.Compression;
using MongoDB.Driver.Core.Configuration;
using System.Collections.Generic;
```

To specify compression with [`MongoClientSettings`]({{<apiref "T_MongoDB_Driver_MongoClientSettings">}}), set the `Compressors` property to a list of [`CompressorConfiguration`]({{<apiref "T_MongoDB_Driver_Core_Configuration_CompressorConfiguration">}}) instances:

```c#
var mongoClientSettings = new MongoClientSettings();
mongoClientSettings.Compressors = new List<CompressorConfiguration>()
{
	new CompressorConfiguration(CompressorType.Snappy)
};
var client = new MongoClient(mongoClientSettings);
```
for Snappy compression, or

```c#
var mongoClientSettings = new MongoClientSettings();
mongoClientSettings.Compressors = new List<CompressorConfiguration>()
{
	new CompressorConfiguration(CompressorType.Zlib)
};
var client = new MongoClient(mongoClientSettings);
```
for zlib compression, or

```c#
var mongoClientSettings = new MongoClientSettings();
mongoClientSettings.Compressors = new List<CompressorConfiguration>()
{
	new CompressorConfiguration(CompressorType.Zstd)
};
var client = new MongoClient(mongoClientSettings);
```
for Zstandard compression, or

```c#
var mongoClientSettings = new MongoClientSettings();
mongoClientSettings.Compressors = new List<CompressorConfiguration>()
{
	new CompressorConfiguration(CompressorType.Snappy),
	new CompressorConfiguration(CompressorType.Zlib),
	new CompressorConfiguration(CompressorType.Zstd)
};
var client = new MongoClient(mongoClientSettings);
```
to configure multiple compressors. 

As with configuration with a URI, the driver will use the first compressor in the list for which the server advertises support.
