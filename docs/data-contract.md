# osm-replication 来源契约

来源清单中的资产长度与 SHA-256 用于核对本地原文字节。文档的 collection 与 protocol 必须匹配清单，现有记录类型包括 sequence, element, version, delete。每条记录以 kind、id、version 定位，refs 指向同集合的其他记录，payload 原样留存。来源版本冲突会中断接收；本地引用闭合只表示当前收到的引用能定位，不表示远端序列或授权资料已经完整。

## 已接收的协议资料
现有入口接收复制状态中的 sequenceNumber 与 timestamp，以及 osmChange 的 create、modify、delete、要素版本、way 的 node 引用与 relation 的 member 引用；这些字段会写入版本账本。

## 连续序列接收（capture_data.replication）

分钟差分不再按时间戳拼接，而以可信复制序列与已核验字节身份推进**单一水位**。规则如下：

1. **字节身份先行**：每条收据核对长度与 SHA-256。身份不符（下载中断、字节被改）的内容绝不入库，仅写 `identity_failure` 审计，可换镜像重试。
2. **state 与 URL 互证**：state.txt 的 `sequenceNumber` 必须与 URL 中的 minute 序列一致；不一致判为 `state_desync`，原始字节隔离留存，水位不推进。
3. **水位即连续前缀**：已确认水位是从锚点起无间断、语义无冲突的最大连续序列。序列缺口处先到的字节标记留存（`trusted/sequence_gap`），缺口补齐后自动晋级；水位永不越过真实缺口。
4. **同序列不同字节**：规范化语义一致则记为字节别名（同一 `content_sha256`，不产生第二状态）；语义不一致则全部候选隔离为 `content_divergence`，需新的独立镜像证据打破平局后才裁决，败选字节以 `content_divergence_lost` 留存。
5. **版本只能前进**：节点、道路、关系的每个版本逐条留存；迟到的旧版本标记 `stale` 且永不覆盖较新状态，删除动作以墓碑形式长期保留为证据。
6. **引用缺口不阻塞采集**：关系/道路引用暂时缺失只登记 `missing`，要素到达后转 `resolved`；引用指向已删除要素记为 `dangling_deleted`。缺口在发布视图显式报告，不把序列当作完整图，也不无限阻塞后续原始差分。
7. **发布位置即采集位置**：发布视图的 `publishable_up_to` 恒等于水位，`resume_at = high_water + 1`。可从任一已确认水位 `rebuild` 出同一要素状态；超出已确认范围的重建请求被拒绝。
8. **可审计、可重放**：`rep_audit` 只追加，记录 mirror_lag、mirror_switch、received、byte_alias、state_desync、content_divergence、promoted、watermark_advance、gap_opened/resolved、stale_version、element_deleted 等事件。同一字节序列重放结果逐字段一致且不新增审计。

## 冲突事务
同集合第二次来源接收保存在 data/conflict-case.json。冲突事务必须保持第一份收据和旧投影。

## 不可信数据处理
下载与解包得到的字节是不可信数据：每个序列放入各自的新空目录，脚本与数据目录分离，路径作为参数传入；读取这些数据的 Python 一律以 `python -I` 运行，避免工作目录中的同名模块被导入。
