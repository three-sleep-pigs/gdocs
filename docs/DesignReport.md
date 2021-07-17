# Design Report

**Team: three-sleepy-pigs**

## Naive gDocs
### 1.System Architecture

### 2. Basic Requirements
* **Multi-Users Collaboration Editing**

  多用户共同编辑时，客户端用websocket的方式来通信。进入编辑页面时，每个用户与后端建立websocket连接，后端为用户生成一个*OnlineExcelWebSocketServer*，其中的属性有：
  1. *tokenMap*：一个 ConcurrentHashMap，用来存放每个连接的客户端对应的*OnlineExcelWebSocketServer*对象。
  2. *session*：与每个客户端的连接会话，需要通过它来给客户端发送数据。
  3. *userId*：客户端的特有标识。
  4. *fileId*,*version*：用户正在编辑的文件的标识。
  5. *edited*：本次连接是否编辑文件。

  在*OnCreate*阶段，从客户端的ws请求路径获取参数，初始化一个*OnlineExcelWebSocketServer*对象，并加入*tokenMap*。

  在*OnMessage*阶段，客户端的操作（如移动光标，编辑单元格数据，编辑单元格字体、颜色等）均会压缩后发送给该*OnlineExcelWebSocketServer*，*OnlineExcelWebSocketServer*转发给所有与该客户端编辑的文件相同的其他客户端；同时对传来的数据进行解压，判断此操作是否修改了文件内容，如果是，将写文件相关操作转发给DFS完成修改，同时将*edited*置1。

  在*OnClose*阶段，*OnlineExcelWebSocketServer*将自身移除*tokenMap*；此外，判断*edited*是否为1，如果为1则添加一条该用户的编辑记录。

* **Editing the Same Position at the Same Time**

  此处参考目前商业化多人在线共同编辑软件腾讯文档进行设计，当两人同时编辑同一单元格时，其中首先完成并提交的客户端的修改会显示在正在输入的另一用户的前端，覆盖另一用户的输入。

* **File Recycle Bin**

  在删除文件时，为了能够回收，并非通过DFS的删除功能删除，而是为文件增加了*deleted*属性，将其置1，在获取文件列表和回收站时根据该属性进行筛选。回收时再将该属性置0。只有清空回收站时，在DFS中真正地删除。
### 3. Advanced Requirements
* **modification log**

  在Multi-Users Collaboration Editing中提到，若*edited*置1则添加一条该用户的编辑记录，其中属性有：
  1. *fileID*: 被编辑文件的ID。
  2. *editor*: 编辑人的用户名。
  3. *length*: 编辑后的文件长度。
  4. *version*: 被编辑文件的版本号。
  5. *operation*: 此次编辑操作的类型，其中，0：创建，1：修改数据，2：删除，3：恢复，4：版本回滚。
  6. *editTime*: 编辑时间
  
  在客户端查看编辑记录时，展示所有编辑记录的列表，其中每一次编辑当时具体的文件内容可以通过 *fileID*，*length*，*version*从DFS中读出，展示编辑后的文件。此功能类似于现有的新浪微博的编辑记录功能。

* **version rollback**
  
  客户端可以根据编辑记录进行版本回滚的控制。通过 *fileID*，*length*，*version*从DFS中读出该次编辑后的文件内容，将其复制到新文件中，并更新文件的版本，在DFS中命名文件为*filename_version.txt*。对文件的后续编辑将在新版本文件中完成。

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
