go-hbase
========

此项目类似https://gitlab.yxapp.in/songwang11/nepenthes-java

由于之前是使用的nepenthes，但是服务不太稳定，超时情况时有发生，所以用golang client重写

API Documentation: http://godoc.org/github.com/Lazyshot/go-hbase

部署在app101：/data/go-hbase，端口：8089

Supported Versions
------------------

HBase >= 0.96.0 

This version of HBase has a backwards incompatible change, which takes full use of protocol buffers for client interactions.

API
-------

restful接口详见server/main.go
