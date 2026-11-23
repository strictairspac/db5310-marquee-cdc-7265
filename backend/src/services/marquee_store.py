"""Connection-scoped InnoDB workspace for one replay."""

from contextlib import contextmanager

from sqlalchemy import text


_TABLES = {
    'marquee_cdc_offsets': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        seq BIGINT UNSIGNED NOT NULL
    ''',
    'marquee_cdc_projection': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        content VARCHAR(240) NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (stream, sign_id)
    ''',
    'marquee_cdc_journal': '''
        receipt INT UNSIGNED PRIMARY KEY,
        batch_no BIGINT UNSIGNED NOT NULL,
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        event_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        operation ENUM('put', 'delete') NOT NULL,
        content VARCHAR(240) NOT NULL,
        state ENUM('pending', 'applied', 'conflict', 'history') NOT NULL,
        reason VARCHAR(240) NOT NULL,
        KEY stream_sequence (stream, seq),
        KEY stream_event (stream, event_id)
    ''',
}


@contextmanager
def isolated_replay(engine):
    if engine.dialect.name != 'mysql':
        raise ValueError('牌面回放需要 MySQL 8.0')
    with engine.connect().execution_options(isolation_level='READ COMMITTED') as conn:
        try:
            with conn.begin():
                for table, columns in _TABLES.items():
                    conn.exec_driver_sql(
                        f'CREATE TEMPORARY TABLE {table} ({columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
                    )
            yield conn
        finally:
            if not conn.invalidated:
                try:
                    conn.rollback()
                    with conn.begin():
                        conn.exec_driver_sql(f'DROP TEMPORARY TABLE IF EXISTS {", ".join(_TABLES)}')
                except Exception:
                    conn.invalidate()
                    raise


def initialize(conn, checkpoints, snapshot):
    with conn.begin():
        conn.execute(text('INSERT INTO marquee_cdc_offsets (stream, seq) VALUES (:stream, :sequence)'), checkpoints)
        if snapshot:
            conn.execute(text('''
                INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
                VALUES (:stream, :sign_id, :content, :sequence)
            '''), snapshot)


def journal_matches(conn, event):
    return conn.execute(text('''
        SELECT receipt, seq, event_id, sign_id, operation, content, state
        FROM marquee_cdc_journal
        WHERE stream = :stream AND (seq = :seq OR event_id = :event_id)
        ORDER BY receipt
    '''), event).mappings().all()


def insert_receipt(conn, event, state, reason):
    conn.execute(text('''
        INSERT INTO marquee_cdc_journal
            (receipt, batch_no, stream, seq, event_id, sign_id, operation, content, state, reason)
        VALUES (:receipt, :batch_no, :stream, :seq, :event_id, :sign_id, :operation, :content, :state, :reason)
    '''), {**event, 'state': state, 'reason': reason})


def quarantine_pending(conn, event):
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'conflict', reason = :reason
        WHERE stream = :stream AND state = 'pending' AND (seq = :seq OR event_id = :event_id)
    '''), {**event, 'reason': f'与接收行 {event["receipt"]} 的事件序号或事件编号重叠，信封内容不同'})


def waiting_events(conn, stream, offset):
    return conn.execute(text('''
        SELECT receipt, stream, seq, sign_id, operation, content, state
        FROM marquee_cdc_journal
        WHERE stream = :stream AND seq > :offset AND state IN ('pending', 'conflict')
        ORDER BY seq, receipt
    '''), {'stream': stream, 'offset': offset}).mappings().all()


def apply_event(conn, event):
    if event['operation'] == 'delete':
        conn.execute(text('''
            DELETE FROM marquee_cdc_projection WHERE stream = :stream AND sign_id = :sign_id
        '''), event)
    else:
        conn.execute(text('''
            INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
            VALUES (:stream, :sign_id, :content, :seq)
            ON DUPLICATE KEY UPDATE content = :content, seq = :seq
        '''), event)
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'applied' WHERE receipt = :receipt
    '''), event)


def read_offsets(conn):
    return dict(conn.execute(text('SELECT stream, seq FROM marquee_cdc_offsets ORDER BY stream')).all())


def advance_offset(conn, stream, seq):
    conn.execute(text('UPDATE marquee_cdc_offsets SET seq = :seq WHERE stream = :stream'),
                 {'stream': stream, 'seq': seq})


def read_projection(conn):
    return conn.execute(text('''
        SELECT stream, sign_id, content, seq FROM marquee_cdc_projection ORDER BY stream, sign_id
    ''')).mappings().all()


def read_journal(conn):
    return conn.execute(text('SELECT * FROM marquee_cdc_journal ORDER BY receipt')).mappings().all()
