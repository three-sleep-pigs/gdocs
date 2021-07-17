# Design Report

**Team: three-sleepy-pigs**

## Naive gDocs

## Distributed File System

### 1. System Architecture

* **Single-Master Architecture**
  ![gfs paper Figure1](./photos/gfs.png)
  *Single-Master DFS Architecture* 的设计跟 GFS 的结构一致。
  *Single-Master DFS Architecture* 由单个 *master*，多个 *chunk server* 和多个 *client* 组成；文件被分成大小合适的 *chunk*，由 *chunk handle* 唯一标识，每个 *chunk* 会存在多个 *chunk server* 上，默认备份的数量是 3， 每个 *chunk* 最少的备份数量是 2。
  *master* 储存着所有文件系统的 metadata，包括文件名，权限信息，文件到 *chunk* 的映射， *chunk* 储存在哪些 *chunk server* 上等。*master* 的一致性是通过定期与 *master* 的备份同步信息并且记录 log 来保证的。
  *chunk server* 会定期发送心跳信号给 *master*，除了让 *master* 知道这个 *chunk server* 还“活着” 外，还会附带 *lease* 信息， *master* 会返会给 *chunk server* 需要删掉的 *garbage*。

* **Multi-Master Architecture**

### 2. Basic Requirements

* **Operation**
* **Chunk**
* **Replication**
* **Fault Tolerance**
* **Consistency**
* **Concurrency Control**
* **Failure Recovery**
  
  ### 3. Advanced Requirements
* **Scalability**
* **Efficiency**
