trident-mssql
=============

A [Storm](http://www.storm-project.net/) `TridentState` implementation for
Microsoft's SQL Server.

This implementation uses the JTDS JDBC driver, and is based on the
[`storm-mysql`](https://github.com/wilbinsc/storm-mysql) implementation.

It uses the MERGE DML statement, so it is only compatible with SQL Server 2008
onwards.
