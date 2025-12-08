from pathlib import Path

TOP_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = TOP_DIR / 'data'


def bytes2str(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f'{n:.1f}{unit}'
        n /= 1024
    return f'{n:.1f}TB'
