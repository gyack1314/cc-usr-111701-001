# osm-replication 来源契约

来源清单中的资产长度与 SHA-256 用于核对本地原文字节。文档的 collection 与 protocol 必须匹配清单，现有记录类型包括 sequence, element, version, delete。每条记录以 kind、id、version 定位，refs 指向同集合的其他记录，payload 原样留存。来源版本冲突会中断接收；本地引用闭合只表示当前收到的引用能定位，不表示远端序列或授权资料已经完整。

## 已接收的协议资料
现有入口接收复制状态中的 sequenceNumber 与 timestamp，以及 osmChange 的 create、modify、delete、要素版本和 way 的 node 引用；这些字段会写入版本账本。尚未处理 gzip 镜像水位与跨序列完整性。

## 已接收的协议资料
现有入口接收复制状态中的 sequenceNumber 与 timestamp，以及 osmChange 的 create、modify、delete、要素版本和 way 的 node 引用；这些字段会写入版本账本。尚未处理 gzip 镜像水位与跨序列完整性。
