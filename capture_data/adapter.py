import json
from xml.etree import ElementTree as ET

def parse_bundle(raw):
    """解析一份复制差分信封，返回 state 与节点/道路/关系的结构化变更。

    任何结构问题都抛 ValueError，由调用方决定隔离或拒绝；本函数不做信任决策。
    """
    item = json.loads(raw.decode('utf-8'))
    state = dict(line.split('=', 1) for line in item['state'].splitlines() if '=' in line)
    if 'sequenceNumber' not in state or 'timestamp' not in state:
        raise ValueError('state 缺少 sequenceNumber 或 timestamp')
    sequence = int(state['sequenceNumber'])
    tree = ET.fromstring(item['osc'])
    if tree.tag != 'osmChange':
        raise ValueError('非复制差分')
    changes = []
    for action in tree:
        if action.tag not in ('create', 'modify', 'delete'):
            raise ValueError('未知变更动作')
        for element in action:
            if element.tag not in ('node', 'way', 'relation'):
                raise ValueError('未知要素类型')
            refs = [f'node/{nd.attrib["ref"]}' for nd in element.findall('nd')]
            refs += [f'{member.attrib["type"]}/{member.attrib["ref"]}'
                     for member in element.findall('member')]
            changes.append({
                'action': action.tag,
                'kind': element.tag,
                'id': int(element.attrib['id']),
                'version': int(element.attrib['version']),
                'refs': refs,
                'tags': {tag.attrib['k']: tag.attrib['v']
                         for tag in element.findall('tag')},
            })
    return {'sequence': sequence, 'timestamp': state['timestamp'],
            'state_text': item['state'], 'changes': changes}

def decode(raw, manifest):
    bundle = parse_bundle(raw)
    records = [{'kind': 'sequence', 'id': str(bundle['sequence']),
                'version': bundle['timestamp'], 'refs': [],
                'payload': {'state': bundle['state_text']}}]
    for ch in bundle['changes']:
        identity = f"{ch['kind']}/{ch['id']}"
        records.append({'kind': 'element', 'id': identity,
                        'version': str(ch['version']), 'refs': list(ch['refs']),
                        'payload': {'action': ch['action'],
                                    'source_sequence': bundle['sequence'],
                                    'tags': ch['tags']}})
    return {'protocol': manifest['protocol'],
            'collection': manifest['collection'], 'records': records}
