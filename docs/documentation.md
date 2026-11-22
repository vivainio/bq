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

The result is written to `site/`. The Documentation workflow also runs [fix-zensical](https://vivainio.github.io/fix-zensical/) before the build, which adds click-to-zoom diagrams, full-screen tables and layout toggles. It copies two files into `docs/assets/reading-tools/` and edits `zensical.toml` on the runner only, so neither is committed, and a local preview does not have them. To see them locally, run `fix-zensical` on a copy of the repository, not on the working tree. Add new chapters to the navigation in `zensical.toml`. The Documentation workflow validates pull requests and builds the book on pushes to `master`. To publish, configure GitHub Pages to use GitHub Actions and run the Documentation workflow manually on `master`.
