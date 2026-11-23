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


def _sample_event(batch, stream, sequence, sign_id, content, operation='put'):
    return dict(batch=batch, stream=stream, sequence=sequence, event_id=f'E-{sequence}',
                sign_id=sign_id, operation=operation, content=content)


SPEC = {
    'name': '灯窗市集 08:00 开市牌面发布',
    'summary': '按档口流的连续序号发布牌面更新，保留缺位与矛盾的证据。',
    'customer': '灯窗市集·营运发布组',
    'entity_labels': ['牌面回放档案', '断点续放', '牌面发布单'],
    'field_labels': {
        'checkpoints': '起始消费位点', 'snapshot': '位点对应的在用牌面快照', 'events': '到货事件',
        'stream': '分区流', 'sequence': '流内序号', 'batch': '到货批号', 'event_id': '事件编号',
        'sign_id': '牌面编号', 'operation': '动作（put / delete）', 'content': '牌面内容',
    },
    'guidance': '录入昨夜位点与完整牌面快照，再按交付次序填写到货批号。同批允许乱序，撤牌动作填 delete，内容留空。',
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
    },
    'expect': {'已提交事件数': 5, '重投忽略数': 2, '矛盾隔离事件数': 3, '历史未核实数': 0,
               '暂缓事件数': 2, '缺口流数': 1, '矛盾阻塞流数': 1, '在用牌面数': 2},
}


def _rows(payload, key, limit):
    value = payload.get(key)
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


def solve(payload, engine):
    validate(payload)
    duplicates = []
    progress = []
    with store.isolated_replay(engine) as conn:
        store.initialize(conn, payload['checkpoints'], payload['snapshot'])
        for batch, events in groupby(_arrival_events(payload['events']), key=itemgetter('batch_no')):
            repeated, committed = _apply_batch(conn, batch, list(events))
            duplicates.extend(repeated)
            progress.extend(committed)
        with conn.begin():
            offsets = [_offset_row(stream, offset, store.waiting_events(conn, stream, offset))
                       for stream, offset in store.read_offsets(conn).items()]
            signs = [{'分区流': row['stream'], '牌面编号': row['sign_id'],
                      '当前内容': row['content'], '来源序号': row['seq']} for row in store.read_projection(conn)]
            journal = store.read_journal(conn)
    events = [_event_row(event) for event in sorted([*journal, *duplicates], key=itemgetter('batch_no', 'receipt'))]
    metrics = {
        '已提交事件数': sum(row['state'] == 'applied' for row in journal),
        '重投忽略数': len(duplicates),
        '矛盾隔离事件数': sum(row['state'] == 'conflict' for row in journal),
        '历史未核实数': sum(row['state'] == 'history' for row in journal),
        '暂缓事件数': sum(row['state'] == 'pending' for row in journal),
        '缺口流数': sum(row['续放状态'] == '缺号待补' for row in offsets),
        '矛盾阻塞流数': sum(row['续放状态'] == '矛盾待核' for row in offsets),
        '在用牌面数': len(signs),
    }
    return {
        'summary': (f'本轮提交 {metrics["已提交事件数"]} 条变更，发布单有 {len(signs)} 块在用牌面。'
                    f'{metrics["缺口流数"]} 条流待补号，{metrics["矛盾阻塞流数"]} 条流待核对矛盾事件。'),
        'metrics': metrics,
        'sections': [
            {'title': '牌面发布单', 'rows': signs},
            {'title': '消费位点与首个阻塞', 'rows': offsets},
            {'title': '分批推进记录', 'rows': progress},
            {'title': '事件接收与处置', 'rows': events},
        ],
    }
