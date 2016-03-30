# ZetaScale Test Framework

## OS Requirements
```sh
CentOS
RHEL (v5.5, v6.5)
Ubuntu
```

## Dependencies
### Centos：
```sh
yum install libevent libevent-devel libaio libaio-devel snappy snappy-devel nc gcc –nogpgcheck
```

### Ubuntu：
```sh
apt-get install libevent-dev
apt-get install libaio-devel
apt-get install snappy
apt-get install netcat
```
```sh
install perl moudual with cpan:
install switch.pm
install Log::Log4perl
```

## Test System Requirement
```sh
Memory: 20GB
SSD/HDD storage: 128GB (Some stress tests needs 300GB storage space)
```

## Setup test environment.
#### Get ZetaScale test framework
```sh
git clone git@inner-source.sandisk.com:18250/zetascale_test_framework.git
cd zetascale_test_framework
```
#### Link zetascale package to zs_sdk (https://github.com/SanDisk-Open-Source/zetascale)
```sh
ln -s <zetascale/package/path> zs_sdk
```
#### Compile test engine
```sh
cd engine
make clean
make
cd ..
```

### Run testcases
#### Run single test case:
```sh
perl run.pl  --verbose --case=t/MPut/01_1cntr_mput_recovery.t
```
#### Run test cases in test suites dir: t/Basic
```sh
perl run.pl  --verbose --case=t/Basic/
```
#### Run all test cases in test dir: t
```sh
perl run.pl  --verbose --case=t/
```

