# flugra

**flu**ent mi**gra**tion — dependency-aware execution manager for native SQL units.

## 設計方針

- Native SQLのみ（DSLなし）
- Transaction unit が基本抽象単位
- 実行順序はdependency graphから導出（グローバルバージョニングではない）
- Conflict-free設計（グローバルシーケンス番号なし）
- Gitは実行のsource of truthではない（ファイルシステムが正）
- 実行は決定論的であること
- 人間がレビュー可能な実行計画

## アーキテクチャ

### Execution Unit
- unitはディレクトリ = トランザクション境界
- ディレクトリ内の`.sql`ファイルはファイル名順に実行
- leaf directory（子ディレクトリにSQLを持たないディレクトリ）がunit
- フラットディレクトリ（SQLファイルのみ、サブディレクトリなし）は自動検出し各ファイルを個別unitとして扱う

### 依存関係解決
- SQLのヒューリスティック解析でテーブルレベルの依存を推定
- `CREATE TABLE` → creates、`ALTER/INSERT/UPDATE/DELETE/JOIN/REFERENCES` → references
- 同名テーブルの重複CREATEは後勝ち（DROP→CREATE パターン対応）
- 実行順序: topological sort + lexical fallback

### Lock File (`flugra.lock`)
- YAML形式、map構造（listではない）→ マージフレンドリー
- checksum + depends_onを保存
- 最終的な実行順序は保存しない（動的に導出）

### Ledger Table (`schema_migrations`)
- PostgreSQL上に適用済みunitを記録
- unit_id, checksum, applied_at

## モジュール構成

| モジュール | 役割 |
|---|---|
| `discovery` | ディレクトリ走査、leaf unit検出、checksum計算、フラットディレクトリ自動検出 |
| `parser` | SQL解析（テーブル作成・参照の抽出） |
| `planner` | 依存グラフ構築、topological sort、cycle検出 |
| `lock` | flugra.lock の生成・読み込み・検証 |
| `executor` | unit実行（単一トランザクション内、raw_sql使用） |
| `ledger` | schema_migrationsテーブル管理 |
| `migration_parser` | 既存マイグレーションファイルからUp/Downセクション抽出 |
| `hooks` | ライフサイクルフック（pre_apply, post_apply） |
| `schema` | DBスキーマダンプ・比較（テーブル、カラム、型、制約、インデックス、ビュー、関数） |
| `cli` | CLIコマンド定義（plan, lock, apply, status, diff, convert） |

## CLIコマンド

- `flugra plan <root>` — unit検出、依存グラフ、実行順序の表示
- `flugra lock <root>` — lock file生成・更新
- `flugra apply <root> --database-url <url>` — lock検証後、未適用unitを実行（hooks対応）
- `flugra status --database-url <url>` — 適用済み/未適用unitの表示
- `flugra diff <root> --database-url <url>` — 一時DBにマイグレーション適用し参照DBとスキーマ比較（hooks対応）
- `flugra convert <source> <output>` — 既存フラットマイグレーションをflugraネイティブ形式に変換
- `--extract-up` (グローバル) — Up sectionのみ抽出（Up/Down形式のマイグレーションファイル対応）

## Hooks (`flugra.hooks.yaml`)

マイグレーションルートに配置。`pre_apply`/`post_apply`でシェルコマンドを実行。
`DATABASE_URL`環境変数で接続先DBを渡す（diffでは一時DBのURL）。

```yaml
pre_apply:
  - command: "psql \"$DATABASE_URL\" -f ./plv8-functions.sql -q"
    description: "Deploy plv8 functions"
post_apply:
  - command: "./scripts/post-migrate.sh"
```

## ビルド・テスト

```sh
cargo build
cargo test
```

## 技術スタック

- Rust (edition 2021)
- clap 4 (CLI、env feature有効)
- sqlx 0.8 (PostgreSQL、chrono feature有効、raw_sql使用)
- serde + serde_yaml (lock file)
- sha2 + hex (checksum)
- chrono (タイムスタンプ)
- BTreeMap を全体的に使用（決定論的順序のため）

## 技術的な注意点

- ALTER TYPE ADD VALUEを含むSQLは自動的にステートメント分割実行（PostgreSQLのトランザクション制約対応）
- executor: 通常はトランザクション内でraw_sql実行、ALTER TYPE ADD VALUE検出時はautocommitモード
