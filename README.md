# OSM 复制序列的跨镜像增量采集与要素状态资料服务

项目接收 osm-replication 来源资料。`capture_data.io` 冻结字节和请求来源，`capture_data.domain` 核对本领域记录结构，`capture_data.journal` 封存来源原文和版本，`capture_data.projection` 显示本地引用缺口。`capture_data.replication` 在这些原语之上实现**连续复制序列接收**：以可信序列和已核验文件身份推进单一水位，逐条保留节点、道路、关系的版本与删除证据，并让已发布视图与可继续采集的位置严格一致。

Python 3.11 以上，无第三方依赖。样例均为合成，不访问真实站点。

## 测试

```sh
# 常规运行
python -m unittest discover -s tests -v
python -m compileall -q capture_data tests

# 隔离模式（推荐）：先把本包装入 site-packages，再以 -I 运行，
# 使读取不可信下载数据的代码不会从工作目录导入同名模块。
python -m pip install -e . --no-deps
python -I -m unittest discover -s tests -v
```

## 单来源核验

```sh
python -m capture_data.cli inspect data/normal.json
python -m capture_data.cli inspect data/protocol-case.json
```

## 连续序列采集

镜像转录 JSON 描述 collection、锚点序列与每条收据（镜像、URL、字节路径、长度、SHA-256）。字节文件按序列存放在各自隔离目录。

```sh
# 接收转录并输出与水位一致的发布视图（含资料缺口与隔离序列）
python -I -m capture_data.cli frontier \
  --db /tmp/journal.sqlite3 \
  --transcript data/replication-demo/transcript.json

# 从任一已确认水位确定性重建要素状态
python -I -m capture_data.cli rebuild --at 2001 \
  --db /tmp/journal.sqlite3 \
  --transcript data/replication-demo/transcript.json

# 查看追加式审计轨迹（镜像落后/切换、字节别名、冲突裁决、水位推进等）
python -I -m capture_data.cli audit \
  --db /tmp/journal.sqlite3 \
  --transcript data/replication-demo/transcript.json
```

账本为持久 SQLite，可跨进程重开继续；同一字节序列重放结果逐字段一致且不新增审计行。

### 信任与水位规则（摘要）

* 字节身份（长度 + SHA-256）不符一律拒绝入库；state 序列与 URL 不符则隔离。
* 水位 = 从锚点起无间断、语义无冲突的最大连续序列，不按时间戳拼接。
* 同序列异字节：语义一致记别名；语义冲突全部隔离，等待独立镜像证据裁决。
* 要素版本只前进，旧版本标 `stale` 不覆盖新状态，删除保留墓碑证据。
* 引用缺失只报告资料缺口，不阻塞后续原始差分留存；发布位置恒等于采集位置。

Dockerfile 提供 Linux 运行环境；`docker build -t source-capture .` 构建，`docker run --rm source-capture python -m unittest discover -s tests -v` 运行测试。
