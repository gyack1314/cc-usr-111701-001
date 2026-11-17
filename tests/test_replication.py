"""连续序列接收的可审计行为测试。

合成夹具，不访问网络。每份下载字节写入独立新目录（不可信数据隔离），
测试只通过收据对象与字节身份进入采集器。
"""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from capture_data.replication import (
    Ingestor, Interrupted, Receipt, Unavailable, connect, expected_sequence)

MIRROR_A = 'mirror-a.example.test'
MIRROR_B = 'mirror-b.example.test'
MIRROR_C = 'mirror-c.example.test'
MIRROR_D = 'mirror-d.example.test'


def minute_url(mirror, sequence):
    p = f'{sequence // 1_000_000:03d}/{sequence // 1000 % 1000:03d}/{sequence % 1000:03d}'
    return f'https://{mirror}/replication/minute/{p}.osc.gz'


def make_bundle(sequence, changes, timestamp='2026-11-16T10\\:20\\:00Z'):
    parts = []
    for action, elems in changes:
        body = []
        for elem in elems:
            kind, eid, version = elem['kind'], elem['id'], elem['version']
            extras = ''.join(f'<nd ref="{r}"/>' for r in elem.get('nds', []))
            extras += ''.join(
                f'<member type="{m.split("/")[0]}" ref="{m.split("/")[1]}"/>'
                for m in elem.get('members', []))
            extras += ''.join(f'<tag k="{k}" v="{v}"/>'
                              for k, v in elem.get('tags', {}).items())
            body.append(f'<{kind} id="{eid}" version="{version}">{extras}</{kind}>')
        parts.append(f'<{action}>{"".join(body)}</{action}>')
    osc = f'<osmChange version="0.6">{"".join(parts)}</osmChange>'
    state = f'sequenceNumber={sequence}\ntimestamp={timestamp}\n'
    return json.dumps({'state': state, 'osc': osc}, ensure_ascii=False).encode('utf-8')


def write_receipt(base, mirror, sequence, raw, corrupt=False):
    """把不可信字节放进该序列专属的新空目录，返回带身份的收据。"""
    seq_dir = Path(base) / f'seq-{sequence}' / mirror
    seq_dir.mkdir(parents=True, exist_ok=True)
    payload = raw + b'X' if corrupt else raw
    (seq_dir / f'bundle-{hashlib.sha256(payload).hexdigest()[:12]}.osc.gz'
     ).write_bytes(payload)
    digest = hashlib.sha256(raw).hexdigest()
    return Receipt(mirror, minute_url(mirror, sequence), sequence,
                   payload, len(raw), digest)


class Pool:
    """内存镜像池：mirror -> sequence -> 收据生成器；可声明落后或中断。"""

    def __init__(self, mirrors):
        self.mirrors = list(mirrors)
        self._sources = {}

    def put(self, mirror, sequence, raw, corrupt=False, base=None):
        self._sources[(mirror, sequence)] = ('receipt',
                                             write_receipt(base, mirror, sequence,
                                                           raw, corrupt))

    def put_lag(self, mirror, sequence):
        self._sources[(mirror, sequence)] = ('lag', None)

    def fetch(self, mirror, sequence):
        kind, value = self._sources[(mirror, sequence)]
        if kind == 'lag':
            raise Unavailable(f'{mirror} 尚未发布 {sequence}')
        return value


class ReplicationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.db = connect(str(Path(self.tmp) / 'journal.sqlite3'))

    def tearDown(self):
        self.db.close()
        self._tmp.cleanup()

    def ingestor(self, anchor=1000):
        return Ingestor(self.db, 'minute-capture', anchor)

    # 1. 连续接收：水位按序列推进，不按时间戳；删除证据逐条保留。
    def test_contiguous_watermark_and_delete_evidence(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [
            ('create', [{'kind': 'node', 'id': 1, 'version': 1},
                        {'kind': 'node', 'id': 2, 'version': 1}]),
            ('create', [{'kind': 'way', 'id': 9, 'version': 1, 'nds': [1, 2]}])])
        b2 = make_bundle(1002, [
            ('modify', [{'kind': 'way', 'id': 9, 'version': 2, 'nds': [1, 2],
                         'tags': {'name': '南路'}}]),
            ('delete', [{'kind': 'node', 'id': 2, 'version': 2}])])
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_A, 1001, b1)),
                         'received')
        self.assertEqual(ing.high_water, 1001)
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_A, 1002, b2)),
                         'received')
        self.assertEqual(ing.high_water, 1002)
        view = ing.publish()
        self.assertEqual(view['resume_at'], 1003)
        self.assertEqual(view['publishable_up_to'], view['high_water'])
        # node/2 被删后，way/9 对它的引用成为悬空引用，图不再闭合但资料不丢。
        self.assertFalse(view['complete_graph'])
        # 删除要素以墓碑形式保留，且引用它的资料缺口被明确报告。
        self.assertTrue(view['elements']['node/2']['deleted'])
        self.assertIn('node/2', view['dangling_deleted_refs'])
        # 原始字节与两个版本均在账本中。
        self.assertEqual(self.db.execute(
            'SELECT COUNT(*) FROM rep_bundles WHERE status=?', ('confirmed',))
            .fetchone()[0], 2)
        versions = self.db.execute(
            'SELECT version FROM rep_elements WHERE element_id=9 ORDER BY version'
        ).fetchall()
        self.assertEqual(versions, [(1,), (2,)])

    # 2. 镜像落后：不消耗序列，切到第二个镜像完成，留下 mirror_lag/switch 审计。
    def test_mirror_lag_then_switch(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])])
        pool = Pool([MIRROR_A, MIRROR_B])
        pool.put_lag(MIRROR_A, 1001)
        pool.put(MIRROR_B, 1001, b1, base=self.tmp)
        event = ing.sync_next(pool)
        self.assertEqual(event, 'received')
        self.assertEqual(ing.high_water, 1001)
        events = {row['event'] for row in ing.audit_trail()}
        self.assertIn('mirror_lag', events)
        self.assertIn('mirror_switch', events)
        row = self.db.execute(
            'SELECT mirror FROM rep_bundles WHERE sequence=1001 AND status=?',
            ('confirmed',)).fetchone()
        self.assertEqual(row[0], MIRROR_B)

    # 3. state.txt 未同步：声明序列与 URL 不一致，隔离且不推进。
    def test_state_desync_quarantine(self):
        ing = self.ingestor()
        bad = make_bundle(1099, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])])
        event = ing.admit(write_receipt(self.tmp, MIRROR_A, 1001, bad))
        self.assertEqual(event, 'quarantined_state_desync')
        self.assertEqual(ing.high_water, 1000)
        # 另一镜像给出正确 state 后，水位恢复，错位字节仍留存可审计。
        good = make_bundle(1001, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])])
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_B, 1001, good)),
                         'received')
        self.assertEqual(ing.high_water, 1001)
        reasons = {r[1] for r in self.db.execute(
            'SELECT sequence, reason FROM rep_bundles WHERE status=?',
            ('quarantined',)).fetchall()}
        self.assertTrue(any(r and r.startswith('state_desync') for r in reasons))

    # 4. 同序列不同字节：语义一致记别名；语义冲突隔离，第三方裁决后恢复。
    def test_same_sequence_byte_divergence(self):
        ing = self.ingestor()
        changes = [('create', [{'kind': 'node', 'id': 1, 'version': 1,
                                'tags': {'k': 'v'}}])]
        canon = make_bundle(1001, changes)
        # 不同字节但同一语义（格式化差异）。
        bundle_obj = json.loads(canon.decode('utf-8'))
        reencoded = json.dumps(bundle_obj, indent=2, ensure_ascii=False).encode('utf-8')
        self.assertNotEqual(canon, reencoded)
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_A, 1001, canon)),
                         'received')
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_B, 1001, reencoded)),
                         'byte_alias')
        self.assertEqual(ing.high_water, 1001)
        self.assertEqual(self.db.execute(
            'SELECT COUNT(*) FROM rep_bundles WHERE sequence=1001').fetchone()[0], 2)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(DISTINCT content_sha256) FROM rep_bundles WHERE sequence=1001"
        ).fetchone()[0], 1)

        # 语义冲突：第三个独立镜像给出不同语义，2:1 不构成严格多数，
        # 已确认被撤销，全部候选隔离，水位回退到锚点。
        divergent = make_bundle(1001, [
            ('create', [{'kind': 'node', 'id': 1, 'version': 1, 'tags': {'k': 'w'}}])])
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_C, 1001, divergent)),
                         'received')
        self.assertEqual(ing.high_water, 1000)
        self.assertTrue(all(r[0] == 'quarantined' for r in self.db.execute(
            'SELECT status FROM rep_bundles WHERE sequence=1001').fetchall()))
        # 第四个独立镜像支持原语义，3:1 严格多数裁决：恢复确认并推进。
        again = json.dumps(bundle_obj, separators=(',', ':')).encode('utf-8')
        self.assertNotEqual(again, canon)
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_D, 1001, again)),
                         'byte_alias')
        self.assertEqual(ing.high_water, 1001)
        view = ing.publish()
        self.assertEqual(view['elements']['node/1']['tags'], {'k': 'v'})
        # 败选字节仍隔离留存，可审计。
        lost = self.db.execute(
            "SELECT reason FROM rep_bundles WHERE sequence=1001 AND status='quarantined'"
        ).fetchall()
        self.assertTrue(any(r[0] == 'content_divergence_lost' for r in lost))

    # 5. 下载中断：身份失败的字节绝不入库，可重试成功。
    def test_interrupted_download_rejected(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])])
        bad = write_receipt(self.tmp, MIRROR_A, 1001, b1, corrupt=True)
        self.assertEqual(ing.admit(bad), 'identity_failure')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rep_bundles')
                         .fetchone()[0], 0)
        self.assertEqual(ing.high_water, 1000)
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_B, 1001, b1)),
                         'received')

    # 6. 乱序到达不阻塞原始留存，缺口补齐后自动晋级。
    def test_out_of_order_held_then_promoted(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [('create', [{'kind': 'node', 'id': 7, 'version': 1}])])
        b2 = make_bundle(1002, [('modify', [{'kind': 'node', 'id': 7, 'version': 2}])])
        # 1002 先到：越过下一个期望序列，原始字节留存但水位不动。
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_A, 1002, b2)),
                         'received')
        self.assertEqual(ing.high_water, 1000)
        held = ing.publish()['held_sequences']
        self.assertTrue(any(h['sequence'] == 1002 for h in held))
        # 1001 补齐后，1002 自动晋级，水位一次补齐。
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_B, 1001, b1)),
                         'received')
        self.assertEqual(ing.high_water, 1002)
        events = [row['event'] for row in ing.audit_trail()]
        self.assertIn('promoted:sequence_gap', events)
        self.assertEqual(ing.publish()['elements']['node/7']['version'], 2)

    # 7. 关系引用暂时缺失：报告资料缺口但不阻塞后续差分，引用到达后缺口解除。
    def test_reference_gap_does_not_block_ingestion(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [
            ('create', [{'kind': 'relation', 'id': 5, 'version': 1,
                         'members': ['way/9']}])])
        b2 = make_bundle(1002, [
            ('create', [{'kind': 'way', 'id': 9, 'version': 1, 'nds': [3]}])])
        ing.admit(write_receipt(self.tmp, MIRROR_A, 1001, b1))
        self.assertEqual(ing.high_water, 1001)
        view1 = ing.publish()
        self.assertFalse(view1['complete_graph'])
        self.assertIn('way/9', view1['missing_refs'])
        ing.admit(write_receipt(self.tmp, MIRROR_A, 1002, b2))
        self.assertEqual(ing.high_water, 1002)
        view2 = ing.publish()
        self.assertNotIn('way/9', view2['missing_refs'])
        self.assertIn('gap_resolved', {r['event'] for r in ing.audit_trail()})

    # 8. 旧版本不能覆盖新版本：迟到差分只作为证据留存，状态保持新版本。
    def test_stale_version_never_overwrites(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [
            ('create', [{'kind': 'node', 'id': 4, 'version': 1, 'tags': {'name': '新'}}])])
        b2 = make_bundle(1002, [
            ('modify', [{'kind': 'node', 'id': 4, 'version': 2, 'tags': {'name': '新二'}}])])
        # 含旧版本的迟到序列（典型重放/镜像回退故障）。
        stale = make_bundle(1003, [
            ('modify', [{'kind': 'node', 'id': 4, 'version': 1, 'tags': {'name': '旧'}}])])
        for b, seq in ((b1, 1001), (b2, 1002), (stale, 1003)):
            ing.admit(write_receipt(self.tmp, MIRROR_A, seq, b))
        self.assertEqual(ing.high_water, 1003)
        view = ing.publish()
        self.assertEqual(view['elements']['node/4']['version'], 2)
        self.assertEqual(view['elements']['node/4']['tags'], {'name': '新二'})
        stale_rows = self.db.execute(
            'SELECT COUNT(*) FROM rep_elements WHERE stale=1').fetchone()[0]
        self.assertEqual(stale_rows, 1)
        self.assertIn('stale_version', {r['event'] for r in ing.audit_trail()})

    # 9. 重放确定性：同一字节序列喂给空库得到同一水位与同一要素状态。
    def test_replay_is_deterministic_and_idempotent(self):
        bundles = [
            make_bundle(1001, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])]),
            make_bundle(1002, [('modify', [{'kind': 'node', 'id': 1, 'version': 2}])]),
            make_bundle(1003, [('delete', [{'kind': 'node', 'id': 1, 'version': 3}])])]
        first = self.ingestor()
        receipts = [write_receipt(self.tmp, MIRROR_A, 1001 + i, b)
                    for i, b in enumerate(bundles)]
        for r in receipts:
            first.admit(r)
        snapshot = first.publish()
        # 再次喂入完全相同的收据（重放）：无新增、无冲突。
        for r in receipts:
            self.assertEqual(first.admit(r), 'duplicate_bytes')
        self.assertEqual(first.publish(), snapshot)
        # 全新库重放同一字节序列：结果逐字段一致。
        db2 = connect(':memory:')
        second = Ingestor(db2, 'minute-capture', 1000)
        for r in receipts:
            second.admit(r)
        self.assertEqual(second.publish()['elements'], snapshot['elements'])
        self.assertEqual(second.high_water, first.high_water)
        db2.close()

    # 10. 从任一已确认水位重建：状态与前缀缺口一致，越界水位被拒绝。
    def test_rebuild_at_any_confirmed_watermark(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [('create', [
            {'kind': 'node', 'id': 1, 'version': 1},
            {'kind': 'way', 'id': 8, 'version': 1, 'nds': [1]}])])
        b2 = make_bundle(1002, [('delete', [{'kind': 'node', 'id': 1, 'version': 2}])])
        b3 = make_bundle(1003, [('create', [
            {'kind': 'node', 'id': 2, 'version': 1,
             'tags': {'name': '补点'}}])])
        for b, seq in ((b1, 1001), (b2, 1002), (b3, 1003)):
            ing.admit(write_receipt(self.tmp, MIRROR_A, seq, b))
        at1 = ing.rebuild(1001)
        self.assertEqual(set(at1['elements']), {'node/1', 'way/8'})
        self.assertFalse(at1['elements']['node/1']['deleted'])
        at2 = ing.rebuild(1002)
        self.assertTrue(at2['elements']['node/1']['deleted'])
        self.assertIn('node/1', at2['dangling_deleted_refs'])
        at3 = ing.rebuild(1003)
        self.assertEqual(at3['elements']['node/2']['tags'], {'name': '补点'})
        # 重建当前水位与发布视图同源。
        self.assertEqual(at3['elements'], ing.publish()['elements'])
        with self.assertRaises(ValueError):
            ing.rebuild(1004)
        with self.assertRaises(ValueError):
            ing.rebuild(999)

    # 12. 缺口报告按重建前缀裁剪：后来补齐的引用不再出现在较早视图之外。
    def test_gap_report_is_prefix_scoped(self):
        ing = self.ingestor()
        b1 = make_bundle(1001, [('create', [
            {'kind': 'relation', 'id': 3, 'version': 1, 'members': ['way/9']}])])
        b2 = make_bundle(1002, [('create', [
            {'kind': 'way', 'id': 9, 'version': 1, 'nds': [5]}])])
        b3 = make_bundle(1003, [('create', [
            {'kind': 'node', 'id': 5, 'version': 1}])])
        for seq, b in ((1001, b1), (1002, b2), (1003, b3)):
            ing.admit(write_receipt(self.tmp, MIRROR_A, seq, b))
        self.assertEqual(ing.rebuild(1001)['missing_refs'], ['way/9'])
        # 1002 时 way/9 已到达，但它引用的 node/5 仍缺。
        self.assertEqual(ing.rebuild(1002)['missing_refs'], ['node/5'])
        self.assertEqual(ing.rebuild(1003)['missing_refs'], [])
        self.assertEqual(ing.rebuild(1003)['dangling_deleted_refs'], [])

    # 13. 损坏的 osmChange 字节隔离留存，不推进水位，后续好字节可恢复。
    def test_malformed_bundle_quarantined(self):
        ing = self.ingestor()
        bad = json.dumps(
            {'state': 'sequenceNumber=1001\ntimestamp=x\n',
             'osc': '<not-osm/>'}, ensure_ascii=False).encode('utf-8')
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_A, 1001, bad)),
                         'quarantined_malformed')
        self.assertEqual(ing.high_water, 1000)
        good = make_bundle(1001, [('create', [
            {'kind': 'node', 'id': 1, 'version': 1}])])
        self.assertEqual(ing.admit(write_receipt(self.tmp, MIRROR_B, 1001, good)),
                         'received')
        self.assertEqual(ing.high_water, 1001)
        held = ing.publish()['held_sequences']
        self.assertTrue(any(h['reason'] == 'malformed' for h in held))

    # 11. 全镜像落后：返回 None 且水位不动，下一轮仍从同一序列继续。
    def test_all_mirrors_lag_is_resumable(self):
        ing = self.ingestor()
        pool = Pool([MIRROR_A, MIRROR_B])
        pool.put_lag(MIRROR_A, 1001)
        pool.put_lag(MIRROR_B, 1001)
        self.assertIsNone(ing.sync_next(pool))
        self.assertEqual(ing.high_water, 1000)
        b1 = make_bundle(1001, [('create', [{'kind': 'node', 'id': 1, 'version': 1}])])
        pool.put(MIRROR_A, 1001, b1, base=self.tmp)
        self.assertEqual(ing.sync_next(pool), 'received')
        self.assertEqual(ing.high_water, 1001)

    # 14. 跨进程持久化：关闭重开后水位、视图、审计逐字段一致，可继续采集。
    def test_persistence_across_reopen(self):
        db_path = Path(self.tmp) / 'persist.sqlite3'
        db = connect(str(db_path))
        ing = Ingestor(db, 'cap', 1000)
        for seq, ver, act in ((1001, 1, 'create'), (1002, 2, 'modify'),
                              (1003, 3, 'delete')):
            bundle = make_bundle(seq, [(act, [
                {'kind': 'node', 'id': 4, 'version': ver, 'tags': {'n': str(ver)}}])])
            ing.admit(write_receipt(self.tmp, MIRROR_A, seq, bundle))
        before = ing.publish()
        audit_len = len(ing.audit_trail())
        db.close()

        reopened = connect(str(db_path))
        ing2 = Ingestor(reopened, 'cap', 1000)
        self.assertEqual(ing2.high_water, 1003)
        self.assertEqual(ing2.publish(), before)
        # 重开本身不产生审计噪声。
        self.assertEqual(len(ing2.audit_trail()), audit_len)
        self.assertTrue(ing2.rebuild(1003)['elements']['node/4']['deleted'])
        # 重开后可继续接收下一条。
        nxt = make_bundle(1004, [('create', [
            {'kind': 'node', 'id': 8, 'version': 1}])])
        self.assertEqual(ing2.admit(write_receipt(self.tmp, MIRROR_A, 1004, nxt)),
                         'received')
        self.assertEqual(ing2.high_water, 1004)
        reopened.close()

    def test_expected_sequence_from_url(self):
        self.assertEqual(expected_sequence(minute_url(MIRROR_A, 1234)), 1234)
        with self.assertRaises(ValueError):
            expected_sequence('https://x.example.test/state.txt')


if __name__ == '__main__':
    unittest.main()
