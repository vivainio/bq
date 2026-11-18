# Building this book

The book uses [Zensical](https://zensical.org/) with Markdown chapters under `docs/` and navigation in `zensical.toml`.

From the repository root, create an isolated environment and preview the book.

```sh
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r requirements-docs.txt
.venv-docs/bin/zensical serve
```

Run a strict build before committing documentation changes.

```sh
.venv-docs/bin/zensical build --clean --strict
```

The result is written to `site/`. Add new chapters to the navigation in `zensical.toml`. The Documentation workflow validates pull requests and builds the book on pushes to `master`. To publish, configure GitHub Pages to use GitHub Actions and run the Documentation workflow manually on `master`.
