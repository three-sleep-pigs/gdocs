# Test Report

**Team: three-sleepy-pigs**

## Naive gDocs

### 1. correctness test

### 2. performance test

### 3. defects and corresponding improvement methods

## Distributed File System

*Distributed File System test* 通过 go test 进行测试。

测试文件见`/code/src/gfs/test/graybox_test.go`

*correctness test* 详情见测试文件中以`Test` 开头函数。
测试的具体内容如下所示，正确性测试全部通过。

* **Master Operations**
  
  + get chunk handle
    ![](./photos/getchunkhandle.png)
  + get replicas
    ![](./photos/getreplica.png)
  + check replicas equality
    ![](./photos/replicaequal.png)
  + get file info
    ![](./photos/getfileinfo.png)

* **Client API**
  
  + create file
    ![create file](./photos/create.png)
  
  + delete file
    ![](./photos/delete.png)
  
  + create and delete concurrently
    ![](./photos/concreate.png)
  
  + rename file
    ![](./photos/rename.png)
  
  + mkdir
    ![](./photos/mkdir.png)
  
  + write and read big data
    ![](./photos/bigdata.png)
  
  + append pad over (if the append would cause the chunk to exceed the maximum size this chunk should be pad and the data should be appended to the next chunk)
    ![](./photos/padover.png)
  
  + concurrent read and append
    ![](./photos/conappend.png)

* **Fault Tolerance**
  
  + shutdown a chunk server during operations
    ![](./photos/shutdown.png)
  
  + shutdown a master during operations (This test only exists in branch multi-master gfs test file)

* **Persistent Tests**
  
  + check consistency after reastarting a master
  + check consistency after reastarting a chunk server![](./photos/persistent.png)

### 2. performance test

*performance test* 通过 go Benchmark 进行测试。详情见测试文件中以`Benchmark` 开头函数。
测试环境为

> goos: darwin
> goarch: amd64
> cpu: Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz

* **read**
  测试中每次读的长度大小为一个*chunk*长度
  ![read test result](./photos/readper.png)
  约为0.006s/op
* **write**
  测试中每次写的长度大小为一个*chunk*长度
  ![write test result](./photos/writeper.png)
  约为0.114s/op
* **append**
  测试中每次*append*的长度大小为一个*chunk*长度
  ![append test result](./photos/appendper.png)
  约为0.112s/op
  
  ### 3. defects and corresponding improvement methods
* **snapshot**
