# SMACC - Smart Cloud Caching for Data Intensive Applications

SMACC is a novel Cloud caching service that can run on application compute nodes (e.g., on Amazon EC2) and cache frequently-used data residing on cloud storage (e.g., Amazon S3, MinIO) into local memory and locally-attached disks (e.g., Amazon EBS).

***

## Installation

Using Debian/Ubuntu (Ubuntu 20.04.2 LTS +) or Windows 10

### Prerequisites

General Prerequisites:
- Java 15
- [Apache Maven 3.6.3+] (sudo apt install maven)

Prerequisites for SMACC operating with Amazon S3:
- AWS Account and user with the following permissions:
   - AmazonS3FullAccess
   - AmazonSNSRole
   - AmazonSNSFullAccess
- Server machine requires a public IP for Amazon SNS notifications to work (if enabled).
  - If it does not have one, then one solution is to download ngrok (https://ngrok.com/download) and start a tunnel using: ngrok http 8000
  - This will create a forwarding tunnel with a public endpoint, for example: https://35bb-46-251-115-185.eu.ngrok.io -> http://localhost:8000 
  - The port needs to match the "sns.local.port" setting in the server configuration.
  - The end point needs to be added in the "sns.notification.hostname.or.endpoint" setting in the server configuration.
  - To avoid ngrok limitations (time limits), sign up for a free ngrok account. On ngrok website, got to API -> Create API Key

Prerequisites for SMACC operating with MinIO:
- Install the MinIO server for Linux (https://min.io/docs/minio/linux/index.html) or Windows (https://min.io/docs/minio/windows/index.html).

### Compilation

Build jar using **MVN**

```bash
mvn clean package
```

***

## Most important configurations

The below configurations work for both Amazon S3 and MinIO.

### server.config.properties

```properties
backend.cloud.storage.system = s3
s3.amazon.endpoint = s3.amazonaws.com
s3.default.region = eu-central-1
s3.default.bucket = smacc
s3.credentials.access.key = AAAAAAAAAAAAAAAAAAAA
s3.credentials.secret.key = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

cache.memory.capacity = 1000000
cache.memory.state = cache/MemState/
cache.disk.volumes.size = 2
cache.disk.volume.0 = cache/DiskData1, cache/DiskState1, 1000000000
cache.disk.volume.1 = cache/DiskData2, cache/DiskState2, 1000000000
```

### client.config.properties

```properties
s3.amazon.endpoint = s3.amazonaws.com
s3.default.region = eu-central-1
s3.default.bucket = smacc
s3.credentials.access.key = AAAAAAAAAAAAAAAAAAAA
s3.credentials.secret.key = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

***

## Usage (General)

1. Start the server using **java**

```bash
java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.server.main.ServerMain -c conf/server.config.properties
```

2. Test the client using **java** - ONLY with Amazon S3

```bash
java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.test.basic.SimpleSmaccTester
```

## Usage (SmaccClientCLI)

1. Start the server using **java**

```bash
java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.server.main.ServerMain -c conf/server.config.properties
```

2. Start the client using **java** and the appropriate options

```bash
java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.client.SmaccClientCLI -c conf/client.config.properties -h
```

3. Take a look at the usage for the options available:

```bash
Usage: java -cp <path_to_jar>/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.client.SmaccClientCLI <options>

This CLI supports the client configuration file insertion and one of the following operations at a time.

Options:
-h, --help                     Display this usage information
-c, --config <conf_file>       Specify the path to the client configuration file
-p, --put <src> <dest>         Request the SMACC server to upload a file or directory to storage
-g, --get <src> <dest>         Request the SMACC server to retrieve an object or a directory of objects from storage, by giving its key
-d, --del <file/dir>           Request the SMACC server to delete an object or a directory of objects from storage, by giving its key
-dc, --del-cache <file/dir>    Request the SMACC server to delete a an object or a directory of objects from cache, by giving its key
-l, --list [file/dir]          Request the SMACC server to list object key-value pairs
-lc, --list-cache [file/dir]   Request the SMACC server to list cached object key-value pairs
-fs, --file-status <file>      Print the status of a specific file on SMACC storage
-cs, --collect-stats           Request the collection of SMACC server statistics
-rs, --reset-stats             Request the SMACC server to reset its statistics
-cc, --clear-cache             Request the SMACC server to clear the cache
-x, --shutdown                 Request the server to shut down

Example: java -cp target/smacc-2.0-jar-with-dependencies.jar edu.cut.smacc.main.client.SmaccClientCLI -c conf/client.config.properties -p ./local/path/file.txt object/path/key.txt

*If a configuration file is provided, pass it as the first argument.
**When using an option with S3/MinIO directory, make sure to end the directory path with a slash (e.g. path/to/dir/).

```

