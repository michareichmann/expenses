from pathlib import Path


def bytes2str(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f'{n:.1f}{unit}'
        n /= 1024
    return f'{n:.0f}TB'


def find_project_root(start: Path | None = None) -> Path:
    start = start or Path(__file__).resolve()

    for parent in [start, *start.parents]:
        if (parent / 'pyproject.toml').exists():
            return parent

    raise RuntimeError('Project root not found')


ROOT_DIR = find_project_root()
DATA_DIR = ROOT_DIR / 'data'
