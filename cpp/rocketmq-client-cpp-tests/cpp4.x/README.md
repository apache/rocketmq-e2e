## Apache RocketMQ E2E 
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)



### RocketMQ E2E Cpp 4.x Test
The project is a rocketmq 4.x cpp client E2E test project built using CMake, and the following shows how to compile and configure it.

#### Install dependent library
The project relies on the following libraries, make sure you have them installed:

* GTest: Used for unit testing.
* Threads: Used for multithreading support.
* ZLIB: Used for compression and decompression support.
* spdlog: Used for logging.
* fmt: Used for format output.
* RocketMQ: Used for interacting with RocketMQ.

If you already have these libraries installed, CMake will find them automatically. If they are not installed, you can install them by following the steps in the link below:

* [GTest](https://github.com/google/googletest/blob/main/googletest/README.md)

* [fmt](https://github.com/fmtlib/fmt)

* [spdlog](https://github.com/gabime/spdlog)

* [RocketMQ](https://github.com/apache/rocketmq-client-cpp)

You can use the following command to help the project find the cmake configuration file to find the installation location of the library if they are installed in a custom directory.
```cmake
list(APPEND CMAKE_MODULE_PATH "custom_directory")
```

#### Compile project
To compile the project, follow these steps:

1. Make sure you have CMake and the required compiler (e.g. g++) installed.

2. Create a build directory under the project root, such as build.

3. Go to the build directory and run the following command:

```shell
cmake ..
make
```
This will generate the executable file.