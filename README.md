This's a deamon put raw JSON's into a ClickHouse's table. Supposed to be used
for logging.
```
go run ./cmd/put2ch/ -ch-dsn 'tcp://127.0.0.1:9000/?database=log' -table-name rows
```

On 3 KRPS:
```
%Cpu(s):  0.2 us,  0.2 sy,  0.0 ni, 99.6 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
KiB Mem : 16316864 total,  5794144 free,  2273764 used,  8248956 buff/cache
KiB Swap:        0 total,        0 free,        0 used. 13465716 avail Mem 

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND                                                                                                  
 8852 clickho+  20   0 11.203g 982.4m  30876 S   2.8  6.2  49:51.36 clickhouse-serv                                                                                          
30743 xaionaro  20   0 2795144 1.083g   8688 S   0.2  7.0   0:16.28 put2ch                                                                                                   
 5525 zabbix    20   0  101596   1176    100 S   0.1  0.0 189:04.67 zabbix_agentd                                                                                            
```
