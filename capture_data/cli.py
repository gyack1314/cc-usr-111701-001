"""命令行：来源核验（inspect）与连续复制序列采集（frontier / rebuild / audit）。

连续序列子命令操作一个持久 SQLite 账本，可跨进程恢复；输入收据以镜像转录
JSON 描述，字节路径指向每个序列各自的隔离目录。
"""
import argparse
import hashlib
import json
from pathlib import Path

from .io import load
from .domain import summarize
from .journal import connect as journal_connect
from .journal import receive
from .projection import overview
from .replication import (Ingestor, Receipt, connect as replication_connect,
                          expected_sequence)


def _cmd_inspect(args):
    path = Path(args.file)
    manifest, digest = load(path)
    summary = summarize(manifest, path.parent)
    c = journal_connect(':memory:')
    source = receive(c, manifest, path.parent)
    print(json.dumps(
        {'manifest_sha256': digest, 'source_sha256': source,
         'summary': summary, 'overview': overview(c, manifest['collection'])},
        ensure_ascii=False, indent=2))


def _load_receipts(path):
    doc = json.loads(Path(path).read_text(encoding='utf-8'))
    base = Path(path).resolve().parent
    receipts = []
    for item in doc['receipts']:
        raw_path = (base / item['path']).resolve()
        if not raw_path.is_relative_to(base.resolve()):
            raise ValueError('收据字节路径越界')
        raw = raw_path.read_bytes()
        receipts.append(Receipt(
            item['mirror'], item['url'],
            item.get('sequence', expected_sequence(item['url'])),
            raw, item['bytes'], item['sha256']))
    return doc, receipts


def _open_ingestor(args):
    c = replication_connect(args.db)
    doc = json.loads(Path(args.transcript).read_text(encoding='utf-8'))
    return c, Ingestor(c, doc['collection'], doc['anchor'])


def _cmd_frontier(args):
    c, ing = _open_ingestor(args)
    _doc, receipts = _load_receipts(args.transcript)
    for receipt in receipts:
        ing.admit(receipt)
    print(json.dumps(ing.publish(), ensure_ascii=False, indent=2, sort_keys=True))
    c.close()


def _cmd_rebuild(args):
    c, ing = _open_ingestor(args)
    print(json.dumps(ing.rebuild(args.at), ensure_ascii=False, indent=2,
                     sort_keys=True))
    c.close()


def _cmd_audit(args):
    c, ing = _open_ingestor(args)
    print(json.dumps(ing.audit_trail(), ensure_ascii=False, indent=2))
    c.close()


def main():
    parser = argparse.ArgumentParser(description='OSM 复制序列采集服务')
    sub = parser.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser('inspect', help='核验单个来源清单与本地引用')
    p_inspect.add_argument('file')
    p_inspect.set_defaults(func=_cmd_inspect)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--db', required=True, help='持久账本 SQLite 路径')
    common.add_argument('--transcript', required=True,
                        help='镜像收据转录 JSON（含 collection/anchor/receipts）')

    p_frontier = sub.add_parser('frontier', parents=[common],
                                help='接收转录并输出与水位一致的发布视图')
    p_frontier.set_defaults(func=_cmd_frontier)

    p_rebuild = sub.add_parser('rebuild', parents=[common],
                               help='从任一已确认水位重建要素状态')
    p_rebuild.add_argument('--at', type=int, required=True)
    p_rebuild.set_defaults(func=_cmd_rebuild)

    p_audit = sub.add_parser('audit', parents=[common],
                             help='输出追加式审计轨迹')
    p_audit.set_defaults(func=_cmd_audit)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
