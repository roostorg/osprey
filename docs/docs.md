# Contribute to these docs

This documentation is built using [mdBook](https://rust-lang.github.io/mdBook/). Pull requests and changes merged into `main` are automatically compiled to catch broken documentation, but this Discord repository does not deploy a separate GitHub Pages site. The canonical published documentation remains the [upstream ROOST Osprey site](https://roostorg.github.io/osprey/).

Documentation can be edited directly in the GitHub web UI for existing pages, or by selecting the **🖉 Suggest an edit** icon at the top of the docs site. To create a new page, be sure to update `SUMMARY.md` as well. Once you're done with your changes, open a pull request for review.

To understand more about how mdBook works, learn about the [anatomy of a book](https://rust-lang.github.io/mdBook/guide/creating.html#anatomy-of-a-book).

## Documentation guidelines

For consistency and ease of contribution across projects, we follow the [ROOST community documentation guidelines](https://roostorg.github.io/community/documentation.html) whenever possible.

## Developing locally

To build the site locally, clone this repository and install `mdbook` (follow the [official installation instructions](https://rust-lang.github.io/mdBook/guide/installation.html)).

Once installed, use the `mdbook` command-line tool from the root of this repo. For example, to automatically start watching, building, and serving the site:

```shell
mdbook serve
```

Then make your changes, preview them in your web browser (at [http://localhost:3000](http://localhost:3000) by default), commit, push, and open a pull request like any other git project. 
