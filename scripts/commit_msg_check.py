#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys

# 1. 定义支持的 Type
# 如果你想支持 build/ci/revert，请在下面加上 |build|ci|revert
TYPES_LIST = ["feat", "fix", "docs", "style", "refactor", "perf", "test", "chore"]
TYPES = r"(" + "|".join(TYPES_LIST) + ")"

# 2. 严格格式: type[scope]: subject
SCOPE = r"\[.+\]"
SEPARATOR = r":\s"
SUBJECT = r".+$"

commit_regex = f"^{TYPES}{SCOPE}{SEPARATOR}{SUBJECT}"

def validate_commit_msg():
    commit_msg_filepath = sys.argv[1]
    
    try:
        with open(commit_msg_filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        with open(commit_msg_filepath, 'rb') as f:
            content = f.read().decode('utf-8')

    first_line = content.strip().split("\n")[0]

    if not re.match(commit_regex, first_line):
        sys.stderr.write(f"""
{'=' * 50}
❌  Commit 格式校验错误
{'=' * 50}
格式要求: <type>[<scope>]: <subject>

Type 说明:
  feat     : 新功能 (feature)
  fix      : 修复 Bug
  docs     : 文档变更 (documentation)
  style    : 格式调整 (不影响代码运行)
  refactor : 代码重构
  perf     : 性能优化 (performance)
  test     : 增加测试
  chore    : 构建过程或辅助工具变动

正确示例:
  feat[user]: 增加用户注册接口
  fix[ui]: 修复按钮颜色显示错误
  docs[readme]: 更新部署文档
{'=' * 50}
""")
        sys.exit(1)

if __name__ == "__main__":
    validate_commit_msg()