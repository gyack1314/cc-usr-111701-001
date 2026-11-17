"""连续复制序列接收：以可信序列与已核验字节身份推进单一水位。

信任规则（只看本地可核验证据，不按时间戳拼接）：
* 每条收据核对长度与 SHA-256；身份不符的字节绝不入库，只留审计。
* 状态文件声明的 sequenceNumber 必须与 URL 序列一致，否则隔离原始字节。
* 水位是单调整数。已确认水位 = 从锚点起无间断、语义无冲突的最大连续序列。
* 同序列不同字节：规范化语义一致则记为字节别名（不产生第二状态）；
  语义不一致则隔离全部候选，等待第三镜像裁决，水位不越过该序列。
* 序列缺口处收到的资料先隔离留存，缺口补齐后按先后顺序自动晋级。
* 要素版本只能前进：旧版本永不覆盖新版本，异常版本标记并审计，删除证据长期保留。
* 引用缺口只标记资料缺口，不阻塞后续原始差分留存；已发布视图与可继续采集位置
  永远是同一个水位。
"""
import hashlib
import json
import re
import sqlite3

from .adapter import parse_bundle

SCHEMA = """
CREATE TABLE IF NOT EXISTS rep_bundles(
  sequence INTEGER,
  digest TEXT,
  mirror TEXT,
  state_timestamp TEXT,
  content_sha256 TEXT,
  status TEXT,
  reason TEXT,
  applied INTEGER DEFAULT 0,
  raw BLOB,
  PRIMARY KEY(sequence, digest)
);
CREATE TABLE IF NOT EXISTS rep_watermark(
  collection TEXT PRIMARY KEY,
  anchor INTEGER,
  high_water INTEGER
);
CREATE TABLE IF NOT EXISTS rep_elements(
  collection TEXT,
  kind TEXT,
  element_id INTEGER,
  version INTEGER,
  action TEXT,
  refs TEXT,
  tags TEXT,
  source_sequence INTEGER,
  source_digest TEXT,
  stale INTEGER,
  PRIMARY KEY(collection, kind, element_id, version, source_sequence)
);
CREATE TABLE IF NOT EXISTS rep_gaps(
  collection TEXT,
  sequence INTEGER,
  ref TEXT,
  status TEXT,
  first_seen_digest TEXT,
  PRIMARY KEY(collection, sequence, ref)
);
CREATE TABLE IF NOT EXISTS rep_audit(
  ord INTEGER PRIMARY KEY AUTOINCREMENT,
  collection TEXT,
  sequence INTEGER,
  mirror TEXT,
  event TEXT,
  digest TEXT,
  detail TEXT
);
"""

_PATH_TAIL = re.compile(r'(\d{3})/(\d{3})/(\d{3})\.osc\.gz$')


class Interrupted(ValueError):
    """下载中断或字节身份不符：字节不可信，调用方可换镜像重试。"""


class Unavailable(Exception):
    """镜像暂时没有该序列（落后），不消耗序列。"""


class Receipt:
    """一次下载的冻结收据。mirror 为镜像标识，raw 为原始字节。"""

    __slots__ = ('mirror', 'url', 'expected_sequence', 'raw', 'bytes', 'sha256')

    def __init__(self, mirror, url, expected_sequence, raw, length, sha256):
        if not isinstance(length, int) or length < 0:
            raise ValueError('资产长度非法')
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ValueError('资产摘要非法')
        self.mirror = mirror
        self.url = url
        self.expected_sequence = expected_sequence
        self.raw = raw
        self.bytes = length
        self.sha256 = sha256.lower()

    def verify(self):
        """核对字节身份，失败即抛 Interrupted，绝不返回未核验字节。"""
        if len(self.raw) != self.bytes:
            raise Interrupted('下载中断：字节长度与清单不符')
        if hashlib.sha256(self.raw).hexdigest() != self.sha256:
            raise Interrupted('下载中断：SHA-256 与清单不符')


def expected_sequence(url):
    """从 minute 差分 URL 推导声明序列，例如 .../000/001/234.osc.gz -> 1234。"""
    m = _PATH_TAIL.search(url)
    if not m:
        raise ValueError('URL 不含 minute 差分序列路径')
    return int(''.join(m.groups()))


def _canonical(bundle):
    """规范化语义指纹：同一份差分的不同压缩/字节表示应得到同一指纹。"""
    body = json.dumps(bundle['changes'], sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'))
    return hashlib.sha256(
        f"{bundle['sequence']}|{body}".encode('utf-8')).hexdigest()


def connect(path):
    c = sqlite3.connect(path)
    c.execute('PRAGMA foreign_keys=ON')
    c.executescript(SCHEMA)
    return c


class Ingestor:
    def __init__(self, c, collection, anchor):
        self.c = c
        self.collection = collection
        row = c.execute('SELECT anchor, high_water FROM rep_watermark WHERE collection=?',
                        (collection,)).fetchone()
        if row is None:
            c.execute('INSERT INTO rep_watermark VALUES(?,?,?)',
                      (collection, anchor, anchor))
            self.anchor = anchor
            self.high_water = anchor
        else:
            self.anchor, self.high_water = row

    # ------------------------------------------------------------------ #
    # 审计
    # ------------------------------------------------------------------ #
    def _audit(self, sequence, mirror, event, digest=None, detail=''):
        self.c.execute(
            'INSERT INTO rep_audit(collection,sequence,mirror,event,digest,detail)'
            ' VALUES(?,?,?,?,?,?)',
            (self.collection, sequence, mirror, event, digest, detail))

    def _already_audited(self, sequence, event, digest=None):
        return self.c.execute(
            'SELECT 1 FROM rep_audit WHERE collection=? AND sequence=? AND event=?'
            ' AND digest IS ?', (self.collection, sequence, event, digest)).fetchone()

    # ------------------------------------------------------------------ #
    # 接收一条已核验收据
    # ------------------------------------------------------------------ #
    def admit(self, receipt):
        """原子接收一条收据：成功即提交持久化，异常回滚并继续向上抛出。"""
        try:
            event = self._admit_txn(receipt)
        except Exception:
            self.c.rollback()
            raise
        self.c.commit()
        return event

    def _admit_txn(self, receipt):
        """接收一条收据并重新仲裁/推进水位，返回审计事件名。永不因异常字节抛出。"""
        seq = receipt.expected_sequence
        try:
            receipt.verify()
        except Interrupted as exc:
            self._audit(seq, receipt.mirror, 'identity_failure', None, str(exc))
            return 'identity_failure'

        digest = hashlib.sha256(receipt.raw).hexdigest()
        if self.c.execute(
                'SELECT 1 FROM rep_bundles WHERE sequence=? AND digest=?',
                (seq, digest)).fetchone():
            # 重放已封存字节：幂等无副作用，不新增审计行，保证重放确定性。
            return 'duplicate_bytes'

        try:
            bundle = parse_bundle(receipt.raw)
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self._insert_bundle(seq, digest, receipt, None, None,
                                'quarantined', 'malformed')
            self._audit(seq, receipt.mirror, 'malformed', digest, str(exc))
            self._arbitrate(seq)
            self._recompute()
            return 'quarantined_malformed'

        if bundle['sequence'] != seq:
            self._insert_bundle(seq, digest, receipt, None, None,
                                'quarantined', 'state_desync')
            self._audit(seq, receipt.mirror, 'state_desync', digest,
                        f"state={bundle['sequence']}")
            self._arbitrate(seq)
            self._recompute()
            return 'quarantined_state_desync'

        content = _canonical(bundle)
        before = {r[0] for r in self.c.execute(
            'SELECT content_sha256 FROM rep_bundles WHERE sequence=?', (seq,))}
        self._insert_bundle(seq, digest, receipt, bundle, content,
                            'quarantined', 'sequence_gap')
        if content in before:
            self._audit(seq, receipt.mirror, 'byte_alias', digest,
                        'same_content_different_bytes')
            result = 'byte_alias'
        else:
            self._audit(seq, receipt.mirror, 'received', digest)
            result = 'received'
        self._arbitrate(seq)
        self._recompute()
        return result

    def _insert_bundle(self, seq, digest, receipt, bundle, content, status, reason):
        timestamp = bundle['timestamp'] if bundle else None
        self.c.execute(
            'INSERT INTO rep_bundles VALUES(?,?,?,?,?,?,?,0,?)',
            (seq, digest, receipt.mirror, timestamp, content, status, reason,
             receipt.raw))

    def _arbitrate(self, seq):
        """对单个序列的全部候选重新裁决语义可信度，但不越过水位做确认。

        结构损坏/state 错位永远隔离；语义一致的候选标记可信（trusted），
        是否进入已确认前缀由 _recompute 依据连续序列决定。
        """
        rows = self.c.execute(
            'SELECT digest, mirror, content_sha256, reason FROM rep_bundles '
            'WHERE sequence=?', (seq,)).fetchall()
        # 结构损坏与 state 错位的候选永远隔离，不参与语义裁决。
        well = [r for r in rows if r[2] is not None]
        for digest, _mirror, _content, reason in rows:
            if reason in ('malformed', 'state_desync'):
                self._set_status(seq, digest, 'quarantined', reason)
        if not well:
            return
        groups = {}
        for digest, mirror, content, _reason in well:
            groups.setdefault(content, []).append((digest, mirror))
        if len(groups) == 1:
            for digest, _mirror in next(iter(groups.values())):
                self._set_status(seq, digest, 'trusted', 'sequence_gap')
            return
        # 多语义：任何单一镜像都不能自证正确，既有字节别名堆出的票数也不算。
        # 胜出内容必须同时满足：独立镜像支持严格过半，且领先第二名至少 2 票，
        # 确保冲突是被新的独立证据打破（2:1、2:1:1 等均继续隔离等待佐证）。
        support = {content: {m for _, m in members}
                   for content, members in groups.items()}
        total_mirrors = len(set().union(*support.values()))
        tally = sorted(((len(mirrors), content)
                        for content, mirrors in support.items()), reverse=True)
        winner = None
        top_votes, top_content = tally[0]
        runner_up_votes = tally[1][0] if len(tally) > 1 else 0
        if top_votes > total_mirrors / 2 and top_votes - runner_up_votes >= 2:
            winner = top_content
        for content, members in groups.items():
            for digest, _mirror in members:
                if winner is None:
                    self._set_status(seq, digest, 'quarantined',
                                     'content_divergence')
                elif content == winner:
                    self._set_status(seq, digest, 'trusted', 'sequence_gap')
                else:
                    self._set_status(seq, digest, 'quarantined',
                                     'content_divergence_lost')

    def _set_status(self, seq, digest, status, reason):
        old = self.c.execute(
            'SELECT status, reason, mirror FROM rep_bundles WHERE sequence=? AND digest=?',
            (seq, digest)).fetchone()
        old_status, old_reason, mirror = old
        if (old_status, old_reason) == (status, reason):
            return
        self.c.execute(
            'UPDATE rep_bundles SET status=?, reason=? WHERE sequence=? AND digest=?',
            (status, reason, seq, digest))
        if old_status == 'confirmed' and status in ('quarantined', 'trusted'):
            self._audit(seq, mirror, 'confirm_revoked', digest, reason or '')
        elif old_status == 'trusted' and status == 'confirmed':
            if old_reason:
                self._audit(seq, mirror, 'promoted:' + old_reason, digest)
        elif old_status == 'quarantined' and status == 'trusted':
            if old_reason and old_reason != 'sequence_gap':
                self._audit(seq, mirror, 'cleared:' + old_reason, digest)

    # ------------------------------------------------------------------ #
    # 水位重算：沿无间断已确认前缀应用账本，支持回退
    # ------------------------------------------------------------------ #
    def _recompute(self):
        """从锚点重新计算连续可信前缀并应用账本，天然支持回退与晋级。"""
        cursor = self.anchor
        nxt = cursor + 1
        while True:
            row = self.c.execute(
                'SELECT digest, mirror, raw, applied, status FROM rep_bundles '
                'WHERE sequence=? AND status IN (?,?) ORDER BY status, digest LIMIT 1',
                (nxt, 'confirmed', 'trusted')).fetchone()
            if row is None:
                break
            digest, mirror, raw, applied, status = row
            if status == 'trusted':
                self._set_status(nxt, digest, 'confirmed', None)
            if not applied:
                bundle = parse_bundle(raw)
                self._apply(nxt, digest, bundle, mirror)
                self.c.execute(
                    'UPDATE rep_bundles SET applied=1 WHERE sequence=? AND digest=?',
                    (nxt, digest))
            if not self._already_audited(nxt, 'watermark_advance', digest):
                self._audit(nxt, mirror, 'watermark_advance', digest,
                            f'high_water={nxt}')
            cursor = nxt
            nxt += 1
        if cursor != self.high_water:
            self._set_watermark(cursor)
        self._resolve_gaps()

    def _set_watermark(self, value):
        self.high_water = value
        self.c.execute('UPDATE rep_watermark SET high_water=? WHERE collection=?',
                       (value, self.collection))

    def _apply(self, seq, digest, bundle, mirror):
        """把一份已确认差分逐条写入版本账本。"""
        deleted_here = set()
        for ch in bundle['changes']:
            refs = json.dumps(ch['refs'], ensure_ascii=False)
            tags = json.dumps(ch['tags'], ensure_ascii=False, sort_keys=True)
            self.c.execute(
                'INSERT OR IGNORE INTO rep_elements VALUES(?,?,?,?,?,?,?,?,?,0)',
                (self.collection, ch['kind'], ch['id'], ch['version'], ch['action'],
                 refs, tags, seq, digest))
            # 版本只能前进：同一要素若已存在更高版本，本条标记 stale，不覆盖状态。
            higher = self.c.execute(
                'SELECT 1 FROM rep_elements WHERE collection=? AND kind=? AND element_id=? '
                'AND version>? AND stale=0 LIMIT 1',
                (self.collection, ch['kind'], ch['id'], ch['version'])).fetchone()
            if higher and not self._already_audited(seq, 'stale_version', digest):
                self.c.execute(
                    'UPDATE rep_elements SET stale=1 WHERE collection=? AND kind=? '
                    'AND element_id=? AND version=? AND source_sequence=?',
                    (self.collection, ch['kind'], ch['id'], ch['version'], seq))
                self._audit(seq, mirror, 'stale_version', digest,
                            f"{ch['kind']}/{ch['id']} v{ch['version']}")
            if ch['action'] == 'delete':
                deleted_here.add(f"{ch['kind']}/{ch['id']}")
            for ref in ch['refs']:
                self._note_gap(seq, digest, ref)
        # 本次删除会让先前资料里对该要素的引用变成悬空，统一在缺口视图动态反映；
        # 这里补记审计，保证删除证据可追踪。
        for key in sorted(deleted_here):
            self._audit(seq, mirror, 'element_deleted', digest, key)

    def _ref_key(self, ref):
        """把 'node/7' 解析为 (kind, int id)；畸形引用返回 None。"""
        kind, sep, eid = ref.partition('/')
        if not sep or kind not in ('node', 'way', 'relation'):
            return None
        try:
            return kind, int(eid)
        except ValueError:
            return None

    def _element_present(self, ref, include_deleted):
        key = self._ref_key(ref)
        if key is None:
            return False
        kind, eid = key
        clause = '' if include_deleted else "AND action!='delete'"
        return self.c.execute(
            f"SELECT 1 FROM rep_elements WHERE collection=? AND kind=? AND element_id=? "
            f"{clause} LIMIT 1", (self.collection, kind, eid)).fetchone() is not None

    def _note_gap(self, seq, digest, ref):
        if self._ref_key(ref) is None:
            return
        if self._element_present(ref, include_deleted=False):
            return
        status = 'dangling_deleted' if self._element_present(
            ref, include_deleted=True) else 'missing'
        self.c.execute(
            'INSERT OR IGNORE INTO rep_gaps VALUES(?,?,?,?,?)',
            (self.collection, seq, ref, status, digest))
        if status == 'missing':
            self._audit(seq, None, 'gap_opened', digest, ref)

    def _resolve_gaps(self):
        """要素到达后关闭仍登记为 missing 的缺口；删除导致的悬空另行报告。"""
        rows = self.c.execute(
            'SELECT sequence, ref FROM rep_gaps WHERE collection=? AND status=?',
            (self.collection, 'missing')).fetchall()
        for seq, ref in rows:
            if self._element_present(ref, include_deleted=False):
                self.c.execute(
                    'UPDATE rep_gaps SET status=? WHERE collection=? AND sequence=? AND ref=?',
                    ('resolved', self.collection, seq, ref))
                if not self._already_audited(seq, 'gap_resolved', None):
                    self._audit(seq, None, 'gap_resolved', None, ref)

    # ------------------------------------------------------------------ #
    # 拉取循环：镜像落后时切换，不消耗序列
    # ------------------------------------------------------------------ #
    def sync_next(self, pool):
        """从镜像池取得高水位的下一条；全部镜像落后返回 None。"""
        nxt = self.high_water + 1
        attempted = []
        for mirror in pool.mirrors:
            try:
                receipt = pool.fetch(mirror, nxt)
            except Unavailable as exc:
                self._audit(nxt, mirror, 'mirror_lag', None, str(exc))
                attempted.append(mirror)
                continue
            except Interrupted as exc:
                self._audit(nxt, mirror, 'identity_failure', None, str(exc))
                attempted.append(mirror)
                continue
            # 在首个成功镜像之前已有镜像尝试失败：这是一次可审计的镜像切换。
            if attempted:
                self._audit(nxt, mirror, 'mirror_switch', None,
                            'after=' + ','.join(attempted))
            event = self.admit(receipt)
            if event == 'identity_failure':
                attempted.append(mirror)
                continue
            return event
        return None

    # ------------------------------------------------------------------ #
    # 从任一已确认水位确定性重建
    # ------------------------------------------------------------------ #
    def rebuild(self, at_sequence=None):
        """重放 [anchor+1, at_sequence] 的已确认账本，重建同一要素状态。"""
        if at_sequence is None:
            at_sequence = self.high_water
        if at_sequence < self.anchor or at_sequence > self.high_water:
            raise ValueError('重建水位超出已确认范围')
        state = {}
        rows = self.c.execute(
            'SELECT kind, element_id, version, action, refs, tags, source_sequence, stale '
            'FROM rep_elements WHERE collection=? AND source_sequence<=? '
            'ORDER BY source_sequence, version',
            (self.collection, at_sequence)).fetchall()
        for kind, eid, version, action, refs, tags, seq, stale in rows:
            if stale:
                continue
            key = f'{kind}/{eid}'
            entry = {'kind': kind, 'id': eid, 'version': version,
                     'deleted': action == 'delete',
                     'refs': json.loads(refs), 'source_sequence': seq}
            if action != 'delete':
                entry['tags'] = json.loads(tags)
            state[key] = entry
        missing, dangling = self._gap_report(state, at_sequence)
        return {'at_sequence': at_sequence, 'anchor': self.anchor,
                'elements': state, 'missing_refs': missing,
                'dangling_deleted_refs': dangling}

    def _gap_report(self, state, at_sequence):
        """依据某一水位下的要素状态动态判定引用缺口。"""
        available = {key for key, v in state.items() if not v['deleted']}
        deleted = {key for key, v in state.items() if v['deleted']}
        missing, dangling = set(), set()
        for key, v in state.items():
            for ref in v['refs']:
                if ref in available:
                    continue
                if ref in deleted:
                    dangling.add(ref)
                else:
                    missing.add(ref)
        # 账本里登记过、但该前缀下仍未闭合的缺口同样报告。
        for ref, st in self.c.execute(
                'SELECT ref, status FROM rep_gaps WHERE collection=? AND sequence<=?',
                (self.collection, at_sequence)).fetchall():
            if ref in available or ref in deleted:
                continue
            missing.add(ref)
        return sorted(missing), sorted(dangling)

    # ------------------------------------------------------------------ #
    # 发布视图：与可继续采集位置严格一致
    # ------------------------------------------------------------------ #
    def publish(self):
        rebuilt = self.rebuild(self.high_water)
        missing, dangling = rebuilt['missing_refs'], rebuilt['dangling_deleted_refs']
        held_rows = self.c.execute(
            "SELECT DISTINCT sequence, COALESCE(reason,'sequence_gap'), status "
            "FROM rep_bundles WHERE status IN ('quarantined','trusted')"
        ).fetchall()
        held = sorted((seq, reason) for seq, reason, _st in held_rows)
        reasons = []
        if missing:
            reasons.append('引用资料缺口:' + ','.join(missing))
        if dangling:
            reasons.append('引用指向已删除要素:' + ','.join(dangling))
        stale_rows = self.c.execute(
            'SELECT COUNT(*) FROM rep_elements WHERE collection=? AND stale=1',
            (self.collection,)).fetchone()[0]
        if stale_rows:
            reasons.append(f'存在 {stale_rows} 条被更高版本压住的旧版本证据')
        gap_sequences = sorted({seq for seq, reason in held
                                if reason == 'sequence_gap' and seq > self.high_water})
        if gap_sequences:
            reasons.append('后续序列缺口，资料留存待补:' + ','.join(map(str, gap_sequences)))
        conflicts = sorted({seq for seq, reason in held
                            if reason in ('content_divergence', 'content_divergence_lost',
                                          'state_desync', 'malformed')})
        if conflicts:
            reasons.append('隔离序列待裁决:' + ','.join(map(str, conflicts)))
        return {
            'collection': self.collection,
            'anchor': self.anchor,
            'high_water': self.high_water,
            'resume_at': self.high_water + 1,
            'publishable_up_to': self.high_water,
            'complete_graph': not missing and not dangling,
            'reasons': reasons,
            'missing_refs': missing,
            'dangling_deleted_refs': dangling,
            'held_sequences': [{'sequence': s, 'reason': r} for s, r in held],
            'elements': rebuilt['elements'],
        }

    def audit_trail(self):
        return [dict(zip(('ord', 'collection', 'sequence', 'mirror', 'event',
                          'digest', 'detail'), row))
                for row in self.c.execute(
                    'SELECT ord,collection,sequence,mirror,event,digest,detail '
                    'FROM rep_audit ORDER BY ord').fetchall()]
