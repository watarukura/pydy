# python_vscode_project_template

## 目的
- Pythonプロジェクトに必要な開発環境用の構成のtemplate

## 前提
- python3
    - python3.7以上を使う
- pyenv
    - bashrc:
      ```bash
      export PATH=$PATH:~/.pyenv/shims
      pyenv rehash >/dev/null ^&1
      ```
    - fish.config:
      ```fish
      set -x PATH ~/.pyenv/shims $PATH
      pyenv rehash >/dev/null ^&1
      ```
- pipenv
    - プロジェクトごとに.venvディレクトリを作成する
        - bash: `export PIPENV_VENV_IN_PROJECT=true`
        - fish: `set -x PIPENV_VENV_IN_PROJECT true`

## ツール
- black
    - 参考: [もうPythonの細かい書き方で議論しない。blackで自動フォーマットしよう](https://blog.hirokiky.org/entry/2019/06/03/202745)
    - 2019/06/08現在、blackを使用するとバージョンエラーが出るため、Pipfileに`prerelease = true`を記載
        - [Pipenvでよく出喰わす問題](https://pipenv-ja.readthedocs.io/ja/translate-ja/diagnose.html)
- pytest
    - 参考: [pytest入門 - 闘うITエンジニアの覚え書き](https://www.magata.net/memo/index.php?pytest%C6%FE%CC%E7)

## オプション
### RDBへの接続
- PyMySQL
    - MySQL で使用する
- pg8000
    - PostgreSQL or AmazonRedshift で使用する
- SQLAlchemy
    - 大規模プロジェクトでのみ使用する
- Pandas
    - 複数RDBMSへの接続やファイルとDBのデータのJOIN等が必要な場合に使用する

### 開発
- Jupyter
    - REPL的に検証したい場合や、検証内容を保存しておきたいときに使用する
