# Contribute to these docs

This documentation site is built using [mdBook](https://rust-lang.github.io/mdBook/) and deployed to GitHub Pages. Changes merged into the `main` branch will automatically be built and deployed.

Documentation can be edited directly in the GitHub web UI for existing pages, or by selecting the **🖉 Suggest an edit** icon at the top of the docs site. To create a new page, be sure to update `SUMMARY.md` as well. Once you're done with your changes, open a pull request for review.

To understand more about how mdBook works, learn about the [anatomy of a book](https://rust-lang.github.io/mdBook/guide/creating.html#anatomy-of-a-book).

## File names

Documentation files should typically be named in [kebab-case], except for specially-handled files like `README.md` and `CONTRIBUTING.md`. If a file is considered part of a subsection, it should be placed in a folder; for example:

- `docs/`
  - `getting-started.md`
  - `user-guide/`
    - `README.md`
    - `faq.md`

Or:

- `docs/`
  - `getting-started.md`
  - `user-guide.md`
  - `user-guide/`
    - `faq.md`

## Images

Images to be used in the documentation should be stored in `docs/images/` and named as concisely as possible. To make them easier to reference in Markdown, avoid spaces or other special characters and use [kebab-case]. Related images can be places in subfolders; for example:

- `docs/`
  - `images/`
    - `overview.png`
    - `specific-feature/`
      - `overview.png`
      - `detail.png`

If there aren't too many images, it may be simpler to keep a more flat directory structure, i.e.:

- `docs/`
  - `images/`
    - `overview.png`
    - `specific-feature.png`
    - `specific-feature-detail.png`
   
Unless there is a need for specific HTML attributes, use Markdown to reference images, e.g.:

```markdown
![Concise but descriptive alt text](docs/images/overview.png)
```

## Developing locally

To build the site locally, clone this repository and install `mdbook` (follow the [official installation instructions](https://rust-lang.github.io/mdBook/guide/installation.html)).

Once installed, use the `mdbook` command-line tool from the root of this repo. For example, to automatically start watching, building, and serving the site:

```shell
mdbook serve
```

Then make your changes, preview them in your web browser (at [http://localhost:3000](http://localhost:3000) by default), commit, push, and open a pull request like any other git project. 

[kebab-case]: https://developer.mozilla.org/en-US/docs/Glossary/Kebab_case
