{{ config(materialized = 'table', tags = ['gold', 'historical', 'replay']) }}

/*
    Explicit, version-controlled UTC replay windows. Bounds are [start, end).
    Observations are scenario context from docs/01-business-problem.md section 9,
    not a complete flood-label dataset.
*/

SELECT *
FROM (VALUES
    (
        'hanoi_historic_rain_2008',
        TIMESTAMPTZ '2008-10-30 00:00:00+00',
        TIMESTAMPTZ '2008-11-03 00:00:00+00',
        'Mua lich su Ha Noi cuoi thang 10/2008',
        'Khoang 600 mm/3 ngay duoc neu lam case kiem tra; khong phai nhan ngap theo ward.',
        'docs/01-business-problem.md#9',
        'historical_replay_windows_v1'
    ),
    (
        'typhoon_yagi_2024',
        TIMESTAMPTZ '2024-09-06 00:00:00+00',
        TIMESTAMPTZ '2024-09-13 00:00:00+00',
        'Mua va lu sau bao Yagi tai Ha Noi',
        'Dung de xem tich luy nhieu ngay; khong dien giai thanh du bao lu song.',
        'docs/01-business-problem.md#9',
        'historical_replay_windows_v1'
    ),
    (
        'late_august_heavy_rain_2025',
        TIMESTAMPTZ '2025-08-25 00:00:00+00',
        TIMESTAMPTZ '2025-08-29 00:00:00+00',
        'Mua lon cuoi thang 8/2025',
        'Case kiem tra rolling va peak theo du lieu model; khong phai tong mua tram quan trac.',
        'docs/01-business-problem.md#9',
        'historical_replay_windows_v1'
    ),
    (
        'widespread_flood_2025_10_08',
        TIMESTAMPTZ '2025-10-07 00:00:00+00',
        TIMESTAMPTZ '2025-10-10 00:00:00+00',
        'Cua so quanh ngay ngap dien rong 08/10/2025',
        'Danh sach diem ngap chua duoc nap vao model nay; khong dung lam ground truth.',
        'docs/01-business-problem.md#9',
        'historical_replay_windows_v1'
    )
) AS replay_windows(
    replay_window_id,
    window_start_utc,
    window_end_utc,
    scenario_name,
    scenario_context,
    declaration_source,
    replay_definition_version
)
