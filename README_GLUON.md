Gluon specific version of SQLiteJDBC
====================================

Introduction
------------

The Gluon specific version of SQLiteJDBC contains a version of SQLCipher with encryption based
on SQLCipher. The SQLCipher sources are taken from a fork (https://github.com/gluonhq/sqlcipher/tree/jdbc) of the Signal SQLCipher repository (https://github.com/signalapp/sqlcipher), which in turn is a fork from the official SQLCipher repository (https://github.com/sqlcipher/sqlcipher).

At the moment, it only provides builds for the major desktop platforms:

* linux x64 and aarch64
* mac x64 and aarch64
* windows x64

Build process
-------------

The build system uses make to compile the SQLCipher native library and the java JDBC source
files. Running a build for your local system boils down to the following steps:

1. Download an archive that contains the amalgamated sources for SQLCipher. They are downloaded
   from the Gluon fork: https://github.com/gluonhq/sqlcipher/releases. The version that is downloaded
   is specified in the `VERSION` file.
2. Unzip the source file inside the `target` folder
3. Compile the `sqlite3.c` source file. Target specific compiler flags can be found in `Makefile.common`
4. Generate the shared library using the linker. Target specific linker flags can be found in `Makefile.common`
5. Copy the shared library in the correct location inside `src/main/resources`
6. Run `mvn package` to create the JDBC jar

Building native library for your system
---------------------------------------

1. The SQLite version to use should be set in the `VERSION` file
2. Run `make native`
3. The shared library will be available in the `target` folder

Building java JDBC sources
--------------------------

1. Run `make package`
2. Get the final jar in the  `target` directory.

Producing a new full release
----------------------------

We use Github Actions to create a release by using the workflow `build-release.yml`. This workflow is
triggered when a new tag is created. The idea of the workflow is that we build a specific jar file for
each supported target architecture. Each of these jars is then published into the Gluon nexus repository,
where the name of the target architecture is reflected in the name of the jar file. In the end we'll have
the following jar files:

* sqlcipher-jdbc-3.39.4.2-darwin-aarch64.jar
* sqlcipher-jdbc-3.39.4.2-darwin-x86_64.jar
* sqlcipher-jdbc-3.39.4.2-linux-aarch64.jar
* sqlcipher-jdbc-3.39.4.2-linux-x86_64.jar
* sqlcipher-jdbc-3.39.4.2-win-x86_64.jar

The workflow will roughly execute the following steps for each target in the configured matrix:

* For each platform, call `make ${matrix.target} package`
* Set the platform classifier as the environment variable `CLASSIFIER`
* Deploy the file to the Gluon nexus repository with `make deploy`