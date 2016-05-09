# Asakusa DAG Toolset
Asakusa DAG Toolset is a common libraries of DAG processor/compiler for [Asakusa Framework](https://github.com/asakusafw/asakusafw) .

This project includes the followings:

* [Asakusa DSL Compiler](https://github.com/asakusafw/asakusafw-compiler) modules for processing DAG
* Asakusa runtime libraries for processing DAG

## How to build
```sh
./mvnw clean package
```

## How to import projects into Eclipse
```sh
./mvnw install eclipse:eclipse -DskipTests
```

And then import projects from Eclipse.

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
