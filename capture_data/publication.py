"""区分已接收差分与可发布地图状态。"""
def assess(rows):
    elements=[r for r in rows if r['kind']=='element']
    deleted={r['id'] for r in elements if r['payload'].get('action')=='delete'}
    available={r['id'] for r in elements if r['payload'].get('action')!='delete'}
    unresolved=sorted({ref for r in elements for ref in r['refs'] if ref.startswith('node/') and ref not in available})
    return {'publishable':False,'reasons':['跨镜像序列及版本尚未核验']+['要素引用尚未接收:'+ref for ref in unresolved],
            'deleted_elements':sorted(deleted),
            'accepted_sequences':sorted({r['payload']['source_sequence'] for r in elements if 'source_sequence' in r['payload']})}
