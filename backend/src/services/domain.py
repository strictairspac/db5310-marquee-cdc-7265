"""Replay ordered change capture for the market's stall signage."""

import re
from itertools import groupby
from operator import itemgetter

from src.services import marquee_store as store


_MAX_SEQUENCE = 2**53 - 1
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')
_ENVELOPE = ('seq', 'event_id', 'sign_id', 'operation', 'content')
_OPERATIONS = {'put': '更新牌面', 'delete': '撤下牌面'}
_STATES = {'pending': '暂缓', 'applied': '已提交', 'conflict': '矛盾隔离', 'history': '历史未核实'}
_LEDGER_LIMIT = 1000
_SIGN_LIMIT = 1000


def _sample_event(batch, stream, sequence, sign_id, content, operation='put'):
    return dict(batch=batch, stream=stream, sequence=sequence, event_id=f'E-{sequence}',
                sign_id=sign_id, operation=operation, content=content)


def _sample_ledger(stream, sequence, *signs):
    return dict(stream=stream, sequence=sequence,
                signs=[dict(sign_id=sign_id, content=content) for sign_id, content in signs])


SPEC = {
    'name': '灯窗市集 08:00 开市牌面发布',
    'summary': '按档口流的连续序号发布牌面更新，保留缺位、矛盾与盘点差额的证据。',
    'customer': '灯窗市集·营运发布组',
    'entity_labels': ['牌面回放档案', '断点续放', '牌面发布单'],
    'field_labels': {
        'checkpoints': '起始消费位点', 'snapshot': '位点对应的在用牌面快照', 'events': '到货事件',
        'ledgers': '上游盘点账', 'stream': '分区流', 'sequence': '流内序号', 'batch': '到货批号',
        'event_id': '事件编号', 'sign_id': '牌面编号', 'operation': '动作（put / delete）',
        'content': '牌面内容', 'signs': '整区在用牌面',
    },
    'guidance': '录入昨夜位点与完整牌面快照，再按交付次序填写到货批号。每张盘点账登记一个分区流的位点和该位点的整区在用牌面，按位点逐点核对。同批允许乱序，撤牌动作填 delete，内容留空。',
    'sample': {
        'checkpoints': [
            {'stream': 'NORTH', 'sequence': 12},
            {'stream': 'SOUTH', 'sequence': 4},
            {'stream': 'WEST', 'sequence': 0},
        ],
        'snapshot': [
            {'stream': 'NORTH', 'sign_id': 'N-07', 'sequence': 12, 'content': '桂花米酿｜22元'},
            {'stream': 'NORTH', 'sign_id': 'N-08', 'sequence': 11, 'content': '青团｜12元'},
            {'stream': 'SOUTH', 'sign_id': 'S-02', 'sequence': 4, 'content': '烤梨｜16元'},
        ],
        'events': [
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'SOUTH', 5, 'S-02', '烤梨｜14元'),
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'WEST', 1, 'W-03', '冰粉｜10元'),
            _sample_event(2, 'NORTH', 16, 'N-10', '杏仁茶｜12元'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜15元'),
            _sample_event(2, 'NORTH', 13, 'N-08', '', 'delete'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜17元'),
            _sample_event(2, 'SOUTH', 7, 'S-02', '烤梨｜13元'),
            _sample_event(2, 'WEST', 2, 'W-03', '', 'delete'),
            _sample_event(3, 'SOUTH', 5, 'S-02', '烤梨｜15元'),
            _sample_event(3, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
        ],
        'ledgers': [
            _sample_ledger('NORTH', 12, ('N-07', '桂花米酿｜22元'), ('N-08', '青团｜12元')),
            _sample_ledger('NORTH', 13, ('N-07', '桂花米酿｜22元')),
            _sample_ledger('NORTH', 14, ('N-07', '桂花米酿｜18元')),
            _sample_ledger('SOUTH', 5, ('S-02', '烤梨｜14元')),
            _sample_ledger('WEST', 1, ('W-03', '冰粉｜10元')),
            _sample_ledger('WEST', 2),
        ],
    },
    'expect': {'已提交事件数': 5, '重投忽略数': 2, '矛盾隔离事件数': 3, '历史未核实数': 0,
               '暂缓事件数': 2, '缺口流数': 1, '矛盾阻塞流数': 1, '在用牌面数': 2,
               '盘点账数': 6, '盘点一致数': 6, '盘点差额数': 0, '待核盘点数': 0,
               '缺牌项数': 0, '多牌项数': 0, '文案差异项数': 0},
}


def _rows(payload, key, limit, required=True):
    value = payload.get(key)
    if value is None and not required:
        return []
    if not isinstance(value, list) or len(value) > limit or any(not isinstance(row, dict) for row in value):
        raise ValueError(f'「{SPEC["field_labels"][key]}」需为最多 {limit} 行的对象数组')
    return value


def _identifier(row, field, label):
    value = row.get(field)
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
    return value


def _number(row, field, minimum, label):
    value = row.get(field)
    if type(value) is not int or not minimum <= value <= _MAX_SEQUENCE:
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 {minimum} 至 {_MAX_SEQUENCE} 的整数')
    return value


def _content(row, operation, label):
    value = row.get('content')
    if operation == 'delete':
        if value != '':
            raise ValueError(f'{label}的撤牌内容须为空字符串')
        return
    if not isinstance(value, str) or not value.strip() or len(value) > 240:
        raise ValueError(f'{label}的牌面内容需为 1–240 字符的非空正文')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError(f'{label}的牌面内容包含无效的 Unicode 字符') from None


def _signs(row, label):
    value = row.get('signs')
    if not isinstance(value, list) or len(value) > _SIGN_LIMIT or any(not isinstance(sign, dict) for sign in value):
        raise ValueError(f'{label}的「{SPEC["field_labels"]["signs"]}」需为最多 {_SIGN_LIMIT} 行的对象数组')
    seen = set()
    result = []
    for index, sign in enumerate(value, 1):
        sign_label = f'{label}牌面第 {index} 行'
        sign_id = _identifier(sign, 'sign_id', sign_label)
        _content(sign, 'put', sign_label)
        if sign_id in seen:
            raise ValueError(f'{sign_label}的牌面 {sign_id} 重复')
        seen.add(sign_id)
        result.append({'sign_id': sign_id, 'content': sign['content']})
    return result


def validate(payload):
    if not isinstance(payload, dict):
        raise ValueError('回放输入需为对象')
    offsets = {}
    for index, row in enumerate(_rows(payload, 'checkpoints', 100), 1):
        label = f'起始位点第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream in offsets:
            raise ValueError(f'{label}的流 {stream} 已登记起始位点')
        offsets[stream] = _number(row, 'sequence', 0, label)
    if not offsets:
        raise ValueError('至少登记一条流的起始位点')

    sign_keys = set()
    for index, row in enumerate(_rows(payload, 'snapshot', 1000), 1):
        label = f'牌面快照第 {index} 行'
        stream = _identifier(row, 'stream', label)
        sign_id = _identifier(row, 'sign_id', label)
        sequence = _number(row, 'sequence', 1, label)
        _content(row, 'put', label)
        if stream not in offsets or sequence > offsets[stream]:
            raise ValueError(f'{label}的序号须处在已登记流的起始位点内')
        if (stream, sign_id) in sign_keys:
            raise ValueError(f'{label}的牌面 {sign_id} 在流 {stream} 中已登记')
        sign_keys.add((stream, sign_id))

    for index, row in enumerate(_rows(payload, 'events', 1000), 1):
        label = f'到货事件第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        _identifier(row, 'event_id', label)
        _identifier(row, 'sign_id', label)
        _number(row, 'batch', 1, label)
        _number(row, 'sequence', 1, label)
        operation = row.get('operation')
        if operation not in ('put', 'delete'):
            raise ValueError(f'{label}的动作须为 put 或 delete')
        _content(row, operation, label)

    ledger_keys = set()
    for index, row in enumerate(_rows(payload, 'ledgers', _LEDGER_LIMIT, required=False), 1):
        label = f'盘点账第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        sequence = _number(row, 'sequence', 0, label)
        if (stream, sequence) in ledger_keys:
            raise ValueError(f'{label}的分区 {stream} 序号 {sequence} 已登记盘点账')
        ledger_keys.add((stream, sequence))
        _signs(row, label)


def validate_ledger(ledger, payload):
    if not isinstance(ledger, dict):
        raise ValueError('补交盘点账需为对象')
    offsets = {row['stream']: row['sequence'] for row in payload['checkpoints']}
    label = '补交盘点账'
    stream = _identifier(ledger, 'stream', label)
    if stream not in offsets:
        raise ValueError(f'{label}的流 {stream} 缺少起始位点')
    sequence = _number(ledger, 'sequence', 0, label)
    return {'stream': stream, 'sequence': sequence, 'signs': _signs(ledger, label)}


def _stage(conn, event, offset):
    matches = store.journal_matches(conn, event)
    for prior in matches:
        if all(prior[key] == event[key] for key in _ENVELOPE):
            return {**event, 'state': 'duplicate', 'reason': f'与接收行 {prior["receipt"]} 的事件信封完全一致'}
    if matches:
        store.quarantine_pending(conn, event)
        store.insert_receipt(conn, event, 'conflict',
                             f'与接收行 {matches[0]["receipt"]} 的事件序号或事件编号重叠，信封内容不同')
    elif event['seq'] <= offset:
        store.insert_receipt(conn, event, 'history', '序号处在起始位点内，缺少原始事件信封供核对')
    else:
        store.insert_receipt(conn, event, 'pending', '')
    return None


def _blocker(waiting, offset):
    if not waiting:
        return '已追平', '', '', '收到的连续事件已处理'
    first = waiting[0]
    if first['seq'] > offset + 1:
        return '缺号待补', offset + 1, first['seq'] - 1, f'已收到后续序号 {first["seq"]}'
    return '矛盾待核', first['seq'], first['seq'], f'核对接收行 {first["receipt"]} 的事件信封'


def _offset_row(stream, offset, waiting):
    state, start, end, detail = _blocker(waiting, offset)
    return {'分区流': stream, '消费位点': offset, '续放状态': state,
            '阻塞起号': start, '阻塞止号': end, '依据': detail}


def _apply_batch(conn, batch, events):
    duplicates = []
    progress = []
    with conn.begin():
        offsets = store.read_offsets(conn)
        for event in events:
            duplicate = _stage(conn, event, offsets[event['stream']])
            if duplicate is not None:
                duplicates.append(duplicate)

        for stream in sorted({event['stream'] for event in events}):
            previous = offset = offsets[stream]
            waiting = store.waiting_events(conn, stream, offset)
            for event in waiting:
                if event['seq'] != offset + 1 or event['state'] != 'pending':
                    break
                store.apply_event(conn, event)
                offset = event['seq']
            if offset != previous:
                store.advance_offset(conn, stream, offset)
            progress.append({'到货批号': batch, '原位点': previous, '提交事件数': offset - previous,
                             **_offset_row(stream, offset, waiting[offset - previous:])})
    return duplicates, progress


def _arrival_events(rows):
    return sorted([
        dict(receipt=index, batch_no=row['batch'], stream=row['stream'], seq=row['sequence'],
             event_id=row['event_id'], sign_id=row['sign_id'], operation=row['operation'], content=row['content'])
        for index, row in enumerate(rows, 1)
    ], key=itemgetter('batch_no'))


def _event_row(event):
    state = event['state']
    return {'接收行': event['receipt'], '到货批号': event['batch_no'], '分区流': event['stream'],
            '序号': event['seq'], '事件编号': event['event_id'], '牌面编号': event['sign_id'],
            '动作': _OPERATIONS[event['operation']], '内容': event['content'],
            '状态': '重投忽略' if state == 'duplicate' else _STATES[state],
            '依据': event['reason'] or ('已随位点提交' if state == 'applied' else '等待本流解除前序阻塞')}


def _execute(payload, engine):
    duplicates = []
    progress = []
    with store.isolated_replay(engine) as conn:
        store.initialize(conn, payload['checkpoints'], payload['snapshot'])
        for batch, events in groupby(_arrival_events(payload['events']), key=itemgetter('batch_no')):
            repeated, committed = _apply_batch(conn, batch, list(events))
            duplicates.extend(repeated)
            progress.extend(committed)
        with conn.begin():
            offsets = dict(store.read_offsets(conn))
            offset_rows = [_offset_row(stream, offset, store.waiting_events(conn, stream, offset))
                           for stream, offset in offsets.items()]
            signs = [{'分区流': row['stream'], '牌面编号': row['sign_id'],
                      '当前内容': row['content'], '来源序号': row['seq']} for row in store.read_projection(conn)]
            journal = store.read_journal(conn)
    return {'offsets': offsets, 'offset_rows': offset_rows, 'signs': signs,
            'journal': journal, 'duplicates': duplicates, 'progress': progress}


def _applied_by_stream(journal):
    groups = {}
    for row in journal:
        if row['state'] != 'applied':
            continue
        groups.setdefault(row['stream'], []).append(row)
    for rows in groups.values():
        rows.sort(key=itemgetter('seq'))
    return groups


def _projection_at(stream, target, snapshot, applied):
    state = {}
    for row in snapshot:
        if row['stream'] == stream and row['sequence'] <= target:
            state[row['sign_id']] = row['content']
    for event in applied.get(stream, ()):
        if event['seq'] > target:
            break
        if event['operation'] == 'delete':
            state.pop(event['sign_id'], None)
        else:
            state[event['sign_id']] = event['content']
    return state


def _evaluate_ledger(ledger, offsets, snapshot, applied, checkpoints):
    stream = ledger['stream']
    sequence = ledger['sequence']
    offset = offsets.get(stream, 0)
    if sequence > offset:
        return {'stream': stream, 'sequence': sequence, 'status': '待核', 'diffs': [],
                'reason': f'流 {stream} 位点 {offset}，盘点序号 {sequence} 尚未到货'}
    checkpoint = checkpoints.get(stream, 0)
    if sequence < checkpoint:
        return {'stream': stream, 'sequence': sequence, 'status': '待核', 'diffs': [],
                'reason': f'盘点序号早于起始位点 {checkpoint}，缺少当时的整区牌面快照'}
    state = _projection_at(stream, sequence, snapshot, applied)
    ledger_signs = {sign['sign_id']: sign['content'] for sign in ledger['signs']}
    diffs = []
    for sign_id in sorted(ledger_signs):
        if sign_id not in state:
            diffs.append({'kind': '缺牌', 'sign_id': sign_id,
                          'ledger_content': ledger_signs[sign_id], 'release_content': ''})
        elif state[sign_id] != ledger_signs[sign_id]:
            diffs.append({'kind': '文案差异', 'sign_id': sign_id,
                          'ledger_content': ledger_signs[sign_id], 'release_content': state[sign_id]})
    for sign_id in sorted(state):
        if sign_id not in ledger_signs:
            diffs.append({'kind': '多牌', 'sign_id': sign_id,
                          'ledger_content': '', 'release_content': state[sign_id]})
    status = '差额' if diffs else '一致'
    reason = '整区牌面与发布单一致' if not diffs else f'整区牌面存在 {len(diffs)} 处差异'
    return {'stream': stream, 'sequence': sequence, 'status': status, 'reason': reason, 'diffs': diffs}


def _ledgers(payload):
    return [{'stream': row['stream'], 'sequence': row['sequence'],
             'signs': [{'sign_id': sign['sign_id'], 'content': sign['content']} for sign in row['signs']]}
            for row in (payload.get('ledgers') or [])]


def _entries_for(payload, result):
    applied = _applied_by_stream(result['journal'])
    checkpoints = {row['stream']: row['sequence'] for row in payload['checkpoints']}
    snapshot = payload.get('snapshot') or []
    return [_evaluate_ledger(ledger, result['offsets'], snapshot, applied, checkpoints)
            for ledger in _ledgers(payload)]


def _inventory_parts(entries):
    ledger_rows = []
    diff_rows = []
    counts = {'盘点账数': len(entries), '盘点一致数': 0, '盘点差额数': 0, '待核盘点数': 0,
              '缺牌项数': 0, '多牌项数': 0, '文案差异项数': 0}
    for entry in entries:
        missing = sum(diff['kind'] == '缺牌' for diff in entry['diffs'])
        extra = sum(diff['kind'] == '多牌' for diff in entry['diffs'])
        text = sum(diff['kind'] == '文案差异' for diff in entry['diffs'])
        counts['缺牌项数'] += missing
        counts['多牌项数'] += extra
        counts['文案差异项数'] += text
        if entry['status'] == '一致':
            counts['盘点一致数'] += 1
        elif entry['status'] == '差额':
            counts['盘点差额数'] += 1
        else:
            counts['待核盘点数'] += 1
        ledger_rows.append({'分区流': entry['stream'], '盘点序号': entry['sequence'],
                            '核对状态': entry['status'], '缺牌数': missing, '多牌数': extra,
                            '文案差异项数': text, '依据': entry['reason']})
        for diff in entry['diffs']:
            diff_rows.append({'分区流': entry['stream'], '盘点序号': entry['sequence'],
                              '差异类型': diff['kind'], '牌面编号': diff['sign_id'],
                              '盘点内容': diff['ledger_content'], '发布单内容': diff['release_content']})
    return ledger_rows, diff_rows, counts


def _blockers(entries):
    blockers = []
    for entry in entries:
        if entry['status'] == '差额':
            blockers.append(f'分区 {entry["stream"]} 序号 {entry["sequence"]} 盘点存在差额')
        elif entry['status'] == '待核':
            blockers.append(f'分区 {entry["stream"]} 序号 {entry["sequence"]} 盘点待核')
    return blockers


def _section(base, title):
    for section in base.get('sections') or []:
        if section.get('title') == title:
            return section
    return {'title': title, 'rows': []}


def _summary(metrics):
    return (f'本轮提交 {metrics.get("已提交事件数", 0)} 条变更，发布单有 {metrics.get("在用牌面数", 0)} 块在用牌面。'
            f'共核对 {metrics.get("盘点账数", 0)} 张盘点账：{metrics.get("盘点差额数", 0)} 张有差额，'
            f'{metrics.get("待核盘点数", 0)} 张待核。'
            f'{metrics.get("缺口流数", 0)} 条流待补号，{metrics.get("矛盾阻塞流数", 0)} 条流待核对矛盾事件。')


def _compose(base, entries):
    metrics = dict(base.get('metrics') or {})
    ledger_rows, diff_rows, counts = _inventory_parts(entries)
    metrics.update(counts)
    blockers = _blockers(entries)
    return {
        'summary': _summary(metrics),
        'metrics': metrics,
        'signable': not blockers,
        'blockers': blockers,
        'sections': [
            _section(base, '牌面发布单'),
            {'title': '盘点账核对', 'rows': ledger_rows},
            {'title': '盘点差异明细', 'rows': diff_rows},
            _section(base, '消费位点与首个阻塞'),
            _section(base, '分批推进记录'),
            _section(base, '事件接收与处置'),
        ],
        'inventory': {'entries': entries},
    }


def solve(payload, engine):
    validate(payload)
    result = _execute(payload, engine)
    journal = result['journal']
    events = [_event_row(event) for event in
              sorted([*journal, *result['duplicates']], key=itemgetter('batch_no', 'receipt'))]
    metrics = {
        '已提交事件数': sum(row['state'] == 'applied' for row in journal),
        '重投忽略数': len(result['duplicates']),
        '矛盾隔离事件数': sum(row['state'] == 'conflict' for row in journal),
        '历史未核实数': sum(row['state'] == 'history' for row in journal),
        '暂缓事件数': sum(row['state'] == 'pending' for row in journal),
        '缺口流数': sum(row['续放状态'] == '缺号待补' for row in result['offset_rows']),
        '矛盾阻塞流数': sum(row['续放状态'] == '矛盾待核' for row in result['offset_rows']),
        '在用牌面数': len(result['signs']),
    }
    base = {'metrics': metrics, 'sections': [
        {'title': '牌面发布单', 'rows': result['signs']},
        {'title': '消费位点与首个阻塞', 'rows': result['offset_rows']},
        {'title': '分批推进记录', 'rows': result['progress']},
        {'title': '事件接收与处置', 'rows': events},
    ]}
    return _compose(base, _entries_for(payload, result))


def check_late_ledger(payload, ledger, engine):
    normalized = validate_ledger(ledger, payload)
    result = _execute(payload, engine)
    applied = _applied_by_stream(result['journal'])
    checkpoints = {row['stream']: row['sequence'] for row in payload['checkpoints']}
    entry = _evaluate_ledger(normalized, result['offsets'], payload.get('snapshot') or [],
                             applied, checkpoints)
    entry['signs'] = normalized['signs']
    return entry


def append_ledger(report, entry):
    entries = list((report.get('inventory') or {}).get('entries') or []) + [entry]
    return _compose(report, entries)
