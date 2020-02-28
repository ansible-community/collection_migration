# $ git log --pretty=format:"%(trailers:key=Co-Authored-By,separator=%x0A)%x0ACo-Authored-By: %an <%ae>%x0ACo-Authored-By: %cn <%ce>" .github/BOTMETA.yml | sed 's#^\(C\|c\)o-\(A\|a\)uthored-\(B\|b\)y:\s*#Co-Authored-By: #' | sort | uniq | less
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import re
import subprocess


def normalize_author(author_entry: str) -> str:
    return re.sub(
        r'^\s*Co-Authored-By:\s*',
        '', author_entry,
        flags=re.I,
    ).strip()

def filter_authors(author_entry: str) -> bool:
    forbidden_emails = tuple(
        f'<{e}>' for e in {
            'ansibot@users.noreply.github.com',
            'noreply@github.com',
        }
    )
    return bool(author_entry) and not author_entry.endswith(forbidden_emails)


def find_all_the_authors(files, repo_path):
    authors_map = {}

    find_authors = partial(find_all_the_authors_for, repo_path=repo_path)

    files_args = ((f, ) for f in files)
    with ThreadPoolExecutor(max_workers=100) as executor:
        authors_map = {
            path: authors
            for path, authors in executor.map(find_authors, files_args)
        }

    return authors_map

def find_all_the_authors_for(paths, repo_path):
    find_cmd = (
        'git', 'log',
        r'--pretty=format:%(trailers:key=Co-Authored-By,separator=%x0A)%x0A%an <%ae>%x0A%cn <%ce>',
        '--',
        *paths,
    )
    authors = subprocess.check_output(
        find_cmd,
        text=True,
        cwd=repo_path,
    ).splitlines()
    authors = (normalize_author(a) for a in filter(bool, authors))
    return paths, set(a for a in authors if filter_authors(a))
