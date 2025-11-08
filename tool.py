#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

# تنظیمات اولیه
ROOT = Path(r"D:\noema")     # مسیر اصلی پروژه
OUTPUT_COMBINED = ROOT / "combined_all.py"     # فایل خروجی کمباین
OUTPUT_TREE = ROOT / "tree_structure.txt"      # فایل خروجی درخت

IGNORED_DIRS = {"__pycache__", ".pytest_cache", ".git" , ".idea", ".pytest_cache" ".venv", "docs:"}
IGNORED_FILES = {"__init__.py"}

# -------------------------------
# توابع کمکی
# -------------------------------
def build_tree(root: Path):
    """
    ساختار درختی را به صورت رشته برمی‌گرداند (به شکل خروجی tree)
    """
    lines = [root.name]

    def _walk(path: Path, prefix=""):
        entries = sorted([e for e in path.iterdir() if e.name not in IGNORED_FILES and e.name not in IGNORED_DIRS],
                         key=lambda p: p.name.lower())

        dirs = [e for e in entries if e.is_dir()]
        files = [e for e in entries if e.is_file()]

        for i, entry in enumerate(dirs + files):
            connector = "└── " if i == len(dirs + files) - 1 else "├── "
            line = prefix + connector + entry.name
            lines.append(line)
            if entry.is_dir():
                extension = "    " if i == len(dirs + files) - 1 else "│   "
                _walk(entry, prefix + extension)

    _walk(root)
    return "\n".join(lines)

def gather_py_files(root: Path):
    """
    پیدا کردن همه‌ی فایل‌های .py (به‌جز موارد نادیده)
    """
    result = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORED_DIRS]
        for fn in filenames:
            if fn in IGNORED_FILES:
                continue
            if fn.endswith(".py"):
                result.append(Path(dirpath, fn))
    result = sorted(result, key=lambda p: str(p.relative_to(root)).lower())
    return result

def combine_py_files(root: Path, output_file: Path):
    """
    ادغام همه‌ی فایل‌های .py در یک فایل واحد.
    """
    py_files = gather_py_files(root)
    py_files = [p for p in py_files if p.resolve() != output_file.resolve()]

    with output_file.open("w", encoding="utf-8", newline="\n") as out:
        out.write("# Combined Python sources\n")
        out.write(f"# Root: {root.resolve()}\n\n")

        for i, p in enumerate(py_files, 1):
            rel = p.relative_to(root)
            out.write("#" * 80 + "\n")
            out.write(f"# File {i}: {rel}\n")
            out.write("#" * 80 + "\n")
            try:
                content = p.read_text(encoding="utf-8")
                out.write(content.rstrip() + "\n\n")
            except Exception as e:
                out.write(f"# [Skipped {rel}: {e}]\n\n")

    print(f"✅ Combined {len(py_files)} Python files into: {output_file}")

# -------------------------------
# اجرای کار
# -------------------------------
if __name__ == "__main__":
    print(f"Scanning root: {ROOT}")

    # ساخت و ذخیره درخت
    tree_text = build_tree(ROOT)
    OUTPUT_TREE.write_text(tree_text, encoding="utf-8")
    print(f"✅ Tree structure saved to: {OUTPUT_TREE}")

    # کمباین همه فایل‌های .py
    combine_py_files(ROOT, OUTPUT_COMBINED)

    print("\n🎉 Done!")
