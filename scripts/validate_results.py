from pathlib import Path
import sys
expected_list = sys.argv[1]
actual_list = sys.argv[2]

expected = set(Path(expected_list).read_text().splitlines()) if Path(expected_list).exists() else set()
actual = set(Path(actual_list).read_text().splitlines()) if Path(actual_list).exists() else set()
expected = (s.replace('ALPACA_input_table_', '').replace('.csv', '') for s in expected)
actual = (s.replace('optimal_', '').replace('all_', '').replace('.csv', '') for s in actual)
missing = sorted(expected - actual)
if missing:
    Path('missing_segments.txt').write_text('\n'.join(missing) + '\n')
    Path('validation_done.token').write_text('failed')
    print('Missing segments: %d' % len(missing))
else:
    Path('validation_done.token').write_text('done')
    print('Validation OK')