import json
from xml.etree import ElementTree as ET
def decode(raw, manifest):
    item=json.loads(raw.decode('utf-8'))
    state=dict(line.split('=',1) for line in item['state'].splitlines() if '=' in line)
    sequence=int(state['sequenceNumber'])
    tree=ET.fromstring(item['osc'])
    if tree.tag!='osmChange':raise ValueError('非复制差分')
    records=[{'kind':'sequence','id':str(sequence),'version':state['timestamp'],'refs':[],
              'payload':{'state':item['state']}}]
    for action in tree:
        if action.tag not in ('create','modify','delete'):raise ValueError('未知变更动作')
        for element in action:
            if element.tag not in ('node','way','relation'):raise ValueError('未知要素类型')
            identity=f'{element.tag}/{element.attrib["id"]}'
            refs=[str(sequence)]+[f'node/{child.attrib["ref"]}' for child in element.findall('nd')]
            records.append({'kind':'element','id':identity,'version':element.attrib['version'],
                            'refs':refs,'payload':{'action':action.tag,'source_sequence':sequence,
                            'tags':{tag.attrib['k']:tag.attrib['v'] for tag in element.findall('tag')}}})
    return {'protocol':manifest['protocol'],'collection':manifest['collection'],'records':records}
