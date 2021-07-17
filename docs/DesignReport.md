# Design Report

**Team: three-sleepy-pigs**

## Naive gDocs

### 1. Introduction

+ 文档格式：sheets

+ 基本功能
  + 用户登录注册：
    + 用户名作为用户身份的唯一标识
    + 用户使用用户名和密码完成注册和登录
  + 浏览文件列表
    + 所有用户可以查看和编辑所有文件
    + 以列表形式呈现了文件的文件名、创建人、最近编辑时间信息
    + 提供了编辑文件和删除文件的操作
  + 多用户同步编辑Excel文件
    + 用户可同步编辑同一共享的excel文件
    + 对文件中的内容、字体、格式等均可同步编辑
    + 显示正在被其他用户编辑的单元格以及用户名
    + 每次编辑操作均会被自动保存并生成编辑记录
  + 浏览回收站及删除和恢复文件
    + 回收站以列表形式呈现被删除的文件的文件名、创建人信息
    + 提供了每个已删除文件的恢复操作
+ 进阶功能
  + 浏览编辑记录
    + 每个文件的编辑记录页面以列表形式呈现了每次编辑的编辑人、编辑时间、编辑类型信息
  + 回滚文件至历史版本
    + 可以将文件回滚至任一历史版本
+ 技术实现
  + 前端：js+react框架，luckysheet插件实现excel格式共享文档
  + 后端：java+springboot框架+MySQL数据库+DFS
  + 前后端通过ajax请求和websocket通信

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
