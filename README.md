# Mit6.824
distribution system


## Lab1

1.完成一个mapReduce框架, 可以对数据进行自定义的map操作和 reduce 操作 ,实现分布式处理 
2.提供容错机制, worker节点挂掉后, master节点有超时机制, 会把worker节点未完成的任务派给其他节点
3.通过动态库插件来更换自定义的函数 
```
go build -buildmode=plugin ../mrapps/wc.go
例如 ,这句话会生成wc.so动态库
程序可以通过加载wc.so里面包含的map函数和reduce函数来对输入进行处理
```

运行方法
1.开一个终端, 进入到src/main下 go run mrmaster.go pg-*.txt
2.开另外一个(或多个) 终端 
3.首先执行 go build -buildmode=plugin ../mrapps/wc.go 生成wc.so文件
4.然后执行go run mrworker.go wc.so
5.在mr-out-*的文件可以看到mapReduce的输出


备注

1. 2-4步 多执行一个就是多一个 worker进程
2. 您可以在/src/main 下直接执行 sh test-mr.sh 来看mapReduce 进行的各种测试

Done at 2020/10/22
