This project implements a MySQL binlog reader in Java using [Netty](https://netty.io/index.html).

Solve same use-cases that [mysql-binlog-connector-java](https://github.com/osheroff/mysql-binlog-connector-java) does but in an async way. That allows to improve performance by decoding packets in parallel.

WIP.