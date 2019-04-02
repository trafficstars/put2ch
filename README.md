This's a deamon put raw JSON's into a ClickHouse's table. Supposed to be used
for logging.
```
go run ./cmd/put2ch/ -ch-dsn 'tcp://127.0.0.1:9000/?database=log' -table-name rows
```

On 3 KRPS:
```
Tasks: 194 total,   1 running, 193 sleeping,   0 stopped,   0 zombie
%Cpu(s): 11.2 us,  0.8 sy,  0.0 ni, 86.3 id,  1.7 wa,  0.0 hi,  0.0 si,  0.0 st
KiB Mem : 16316864 total,  4081240 free,  2947736 used,  9287888 buff/cache
KiB Swap:        0 total,        0 free,        0 used. 12791728 avail Mem 

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND                                                                                                  
 8852 clickho+  20   0 11.310g 1.187g  31476 S  87.5  7.6  51:59.70 clickhouse-serv                                                                                          
  424 xaionaro  20   0 4944000 1.487g   7944 S   6.2  9.6   0:21.95 put2ch                                                                                                   
24646 root      20   0       0      0      0 S   0.5  0.0   0:04.29 kworker/u16:3                                                                                                                                                                         
```
