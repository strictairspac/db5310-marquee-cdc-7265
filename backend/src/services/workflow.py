import hashlib
import uuid

from fastapi import HTTPException
from sqlalchemy import and_, or_, select

from src.config.settings import cache, engine
from src.constants.workflow import Status
from src.models.tables import audit, cases, evidence, inventory_ledgers, runs
from src.services import domain
from src.utils.encoding import canonical
from src.utils.time import utc_time


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def enqueue(case_id, actor):
    with engine.begin() as conn:
        found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass
    return result


def claim():
    ready = or_(runs.c.status == Status.QUEUED,
                and_(runs.c.status == Status.RUNNING, runs.c.started_at < utc_time(-600)))
    with engine.begin() as conn:
        run = conn.execute(select(runs.c.id, runs.c.case_id).where(ready).order_by(runs.c.id)
                           .limit(1).with_for_update(skip_locked=True)).first()
        if not run:
            return None
        token = str(uuid.uuid4())
        conn.execute(runs.update().where(runs.c.id == run.id).values(
            started_at=utc_time(), status=Status.RUNNING, attempt_token=token))
        return run.id, token, found(conn, cases, run.case_id)['payload']


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error)).rowcount
        if not count:
            return
        if result is not None:
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
        log(conn, 'worker', status.value, run_id)


def _evidence_case(conn, row):
    run = conn.execute(select(runs).where(runs.c.id == row['run_id'])).mappings().first()
    if run is None:
        raise HTTPException(404, '找不到该证据对应的执行')
    case = conn.execute(select(cases).where(cases.c.id == run['case_id'])).mappings().first()
    if case is None:
        raise HTTPException(404, '找不到该证据对应的档案')
    return case['payload']


def _lock_evidence(conn, identity):
    row = conn.execute(select(evidence).where(evidence.c.id == identity).with_for_update()).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return row


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        row = _lock_evidence(conn, identity)
        if row['reviewed_at'] is not None:
            raise HTTPException(409, '该证据已经复核')
        report = row['report'] or {}
        if report.get('signable') is False:
            blockers = report.get('blockers') or []
            detail = '；'.join(blockers)
            raise HTTPException(409, '发布单存在盘点差额或待核盘点，不能签署。' + (detail and f'{detail}。' or ''))
        conn.execute(evidence.update().where(evidence.c.id == identity).values(
            review_note=note, reviewer=actor, reviewed_at=utc_time()))
        log(conn, actor, 'review', identity)
        return dict(_lock_evidence(conn, identity))


def submit_ledger(identity, ledger, actor):
    with engine.connect() as conn:
        row = conn.execute(select(evidence).where(evidence.c.id == identity)).mappings().first()
        if row is None:
            raise HTTPException(404, '找不到该记录')
        payload = _evidence_case(conn, row)
    entry = domain.check_late_ledger(payload, ledger, engine)
    with engine.begin() as conn:
        row = _lock_evidence(conn, identity)
        if row['reviewed_at'] is not None:
            raise HTTPException(409, '发布单已签署，补交盘点记录不能再入账')
        report = row['report'] or {}
        existing = (report.get('inventory') or {}).get('entries') or []
        if any(item['stream'] == entry['stream'] and item['sequence'] == entry['sequence'] for item in existing):
            raise HTTPException(409, '该分区位点的盘点账已经入账')
        report = domain.append_ledger(report, entry)
        digest = hashlib.sha256(canonical(report)).hexdigest()
        conn.execute(evidence.update().where(evidence.c.id == identity)
                     .values(report=report, digest=digest))
        conn.execute(inventory_ledgers.insert().values(
            evidence_id=identity, stream=entry['stream'], sequence=entry['sequence'],
            signs=entry['signs'], submitted_by=actor, created_at=utc_time()))
        log(conn, actor, 'submit_ledger', identity)
        return dict(_lock_evidence(conn, identity))


def list_ledgers(identity):
    with engine.connect() as conn:
        found(conn, evidence, identity)
        rows = conn.execute(select(inventory_ledgers).where(inventory_ledgers.c.evidence_id == identity)
                            .order_by(inventory_ledgers.c.id)).mappings()
        return [dict(row) for row in rows]
